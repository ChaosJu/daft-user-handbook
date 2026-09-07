# KubeRay 实战清单（RayJob 形态）

这套 YAML 是把一个真实跑过的 Daft on Ray 音频流水线基准测试，
按 [KubeRay RayJob Quickstart](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html)
的形态重新组织的结果。参数不是示意值：`10 × 2C/10Gi`、`object-store-memory 2Gi`、
`/dev/shm 3Gi`、`asr-actor-concurrency 16`、`default_morsel_size 8`
都来自那次压测的实际配置。

环境基线：KubeRay v1.6.2、Ray 2.55.1、镜像 `daft-audio:offline`、namespace `daft-bench`。

配套讲解见 [RayJob 实战](../../docs/03-best-practices/rayjob-hands-on.md)。

> 手上还没有自建镜像，只想确认 KubeRay 和 RayJob 装对了？先跑 [`../quickstart/`](../quickstart/)：两个文件、官方 `rayproject/ray` 镜像、官方示例脚本，零构建。

## 改造前后对照

原来的形态是"常驻 RayCluster + 外部 submit Job"，五个文件各管一段生命周期。
改成 RayJob 之后：

| 原文件 | 去处 | 说明 |
| --- | --- | --- |
| `00-platform.yaml` | `00-platform.yaml` | 保留。另加 `run-bench.sh` / `publish-artifacts.py`，`wait.py` 去掉 ray/dashboard 探针 |
| `05-daft-dashboard.yaml` | `05-daft-dashboard.yaml` | 保留。独立 Deployment 这个选择在 RayJob 下从"更好"变成"必须" |
| `20-generate-job.yaml` | `10-generate-job.yaml` | 只改编号。单进程 + 只依赖 MinIO，**不该**做成 RayJob |
| `10-raycluster.yaml` | `20-rayjob.yaml` 的 `spec.rayClusterSpec` | 集群规格整体内联 |
| `30-submit-job.yaml` | `20-rayjob.yaml` 的 `spec.entrypoint` + `spec.submitterPodTemplate` | 手写的 `JobSubmissionClient` / tail / 状态检查全部由 KubeRay 接管 |
| 无 | `30-raycronjob.yaml` | 新增能力：`RayCronJobSpec.jobTemplate` 的类型就是 `RayJobSpec` |
| `10-raycluster.yaml` | `40-raycluster.yaml` + `41-rayjob-existing.yaml` | 保留常驻集群这条路，但提交方式也换成 RayJob（`clusterSelector`） |

## 文件清单

| 文件 | 类型 | 作用 |
| --- | --- | --- |
| `00-platform.yaml` | Namespace / Secret / ConfigMap ×2 / MinIO / Mock LLM | 常驻依赖，apply 一次 |
| `05-daft-dashboard.yaml` | Deployment + Service | 可选，Daft 查询级观测（`:3238`） |
| `10-generate-job.yaml` | `batch/v1` Job | 生成输入数据集到 MinIO |
| `20-rayjob.yaml` | `ray.io/v1` RayJob | **主入口**，临时集群跑完即拆 |
| `30-raycronjob.yaml` | `ray.io/v1` RayCronJob | 可选，每晚小规模回归 |
| `40-raycluster.yaml` | `ray.io/v1` RayCluster | 可选，常驻集群（调参用） |
| `41-rayjob-existing.yaml` | `ray.io/v1` RayJob | 配 `40` 用，`clusterSelector` 打进常驻集群 |

## 两条路径，按需选一条

`20` 和 `40`+`41` 抢同一批节点，**不要同时 apply**。

| | 临时集群 | 常驻集群 |
| --- | --- | --- |
| 文件 | `20-rayjob.yaml` | `40-raycluster.yaml` + `41-rayjob-existing.yaml` |
| 关键字段 | `spec.rayClusterSpec` | `spec.clusterSelector` |
| 每次启动开销 | 2–5 分钟拉集群 | 秒级，集群是热的 |
| 空闲成本 | 0 | 100Gi 一直占着 |
| 作业失败后 | 集群没了，只能看日志和 S3 产物 | 集群还在，能 `exec` 进 head 翻 `/tmp/ray` |
| 自动回收 | `shutdownAfterJobFinishes: true` | **不支持**，见下 |
| 适合 | 生产批处理、定时任务 | 连续调参、交互排查、严格独占节点做对比基准 |

`clusterSelector` 模式有四条 KubeRay 硬约束，不是风格选择：

- `shutdownAfterJobFinishes` **静默忽略**。controller 在作业进终态时先判 `clusterSelector` 就 `return` 了（"we must not delete it"），写 `true` 既不生效也不报错。常驻集群只能手动 `kubectl delete raycluster`。
- `backoffLimit` 只能是 `0`（"BackoffLimit is incompatible with ClusterSelector mode"）。作业级重试的语义是"新建一整个集群重跑"，这个模式下没有集群可建。
- `suspend` 不支持，所以这条路径没法交给 Kueue 排队。
- `submissionMode` 不能是 `SidecarMode`。

调参循环长这样：

```bash
# 集群起一次，之后一直热着
kubectl apply -f 40-raycluster.yaml
kubectl get raycluster daft-audio -n daft-bench -w      # 等 STATUS=ready

# 每轮：改 41 里的 actor/partition 参数 + 换 metadata.name，再 apply
kubectl apply -f 41-rayjob-existing.yaml

# 调完删集群，回到 20-rayjob.yaml
kubectl delete raycluster daft-audio -n daft-bench
```

`40-raycluster.yaml` 保留了原版的硬性 `nodeSelector` + `podAntiAffinity` + `DoNotSchedule`，apply 前要先给**刚好 10 个**节点打标签：

```bash
for n in node-01 node-02 node-03 node-04 node-05 \
         node-06 node-07 node-08 node-09 node-10; do
  kubectl label node "$n" daft-bench/ray-worker=true --overwrite
done
```

常驻集群里 Pending 是想要的信号（人会看到并处理）。`20-rayjob.yaml` 把这套放宽成 `ScheduleAnyway`，因为 RayJob 要等集群 ready 才提交，一个 Pod 排不下就会一路拖到 `preRunningDeadlineSeconds` 才失败。

## apply 之前必须改的三处

1. `00-platform.yaml` 里 MinIO 的 `nodeSelector`：`REPLACE_WITH_NODE_NAME` 换成有大容量 `/data` 的节点名。hostPath 是节点本地数据，不能漂。
2. 所有 `daft-audio:offline` 换成你的离线 registry 路径，并放开 `imagePullSecrets`。
3. RayJob 的 `entrypoint` 里 `REPLACE_WITH_DATA_RUN_ID` 换成第 2 步打印的 `GEN_RUN_ID`（`20-rayjob.yaml`、`41-rayjob-existing.yaml`、`30-raycronjob.yaml` 各自都有一处）。

## 执行顺序

```bash
kubectl apply -f 00-platform.yaml
kubectl apply -f 05-daft-dashboard.yaml            # 可选

kubectl apply -f 10-generate-job.yaml
kubectl logs -n daft-bench job/bench-generate -f   # 记下 GEN_RUN_ID

# 把 GEN_RUN_ID 填进 20-rayjob.yaml 的 entrypoint
kubectl apply -f 20-rayjob.yaml
```

走常驻集群那条路的话，最后两步换成：

```bash
kubectl apply -f 40-raycluster.yaml
kubectl get raycluster daft-audio -n daft-bench -w   # 等 STATUS=ready
kubectl apply -f 41-rayjob-existing.yaml             # GEN_RUN_ID 填这里
```

## 观察

```bash
# 状态机：jobDeploymentStatus 走 Initializing → Running → Complete/Failed
kubectl get rayjob daft-audio-bench -n daft-bench -w

# driver 日志（跑在 head 上）
kubectl logs -n daft-bench -l ray.io/node-type=head -f

# 提交器日志（ray job submit 的输出，含最终判定）
kubectl logs -n daft-bench -l job-name=daft-audio-bench -f

# 失败原因（SubmissionFailed / DeadlineExceeded / PreRunningDeadlineExceeded / AppFailed）
kubectl get rayjob daft-audio-bench -n daft-bench -o jsonpath='{.status.reason}{"\n"}{.status.message}{"\n"}'
```

临时集群的名字带随机后缀，不要写死。从 RayJob 状态里取（这条命令对两条路径都成立，`clusterSelector` 模式下它就是 `daft-audio`）：

```bash
CL=$(kubectl get rayjob daft-audio-bench -n daft-bench -o jsonpath='{.status.rayClusterName}')
kubectl -n daft-bench port-forward "svc/${CL}-head-svc" 8265:8265
```

## 产物

集群跑完就删，所以产物一律在 S3，不在 Pod 里：

- 报表与采样：`s3://benchmark/audio/<RUN_ID>/artifacts/`（`report.md`、`resources.csv`）
- 输出数据：`s3://benchmark/audio/<RUN_ID>/output.lance`
- 输入 manifest：`s3://benchmark/audio/<DATA_RUN_ID>/manifest.parquet`

`RUN_ID` 由 `run-bench.sh` 按 UTC 时间生成，driver 日志开头会打印。

## 重跑

RayJob 是一次性对象，改了 spec 必须先删再建：

```bash
kubectl delete rayjob daft-audio-bench -n daft-bench --ignore-not-found
kubectl apply -f 20-rayjob.yaml
```

同一份数据集复跑不需要重新生成，跳过 `10-generate-job.yaml`，`DATA_RUN_ID` 保持不变即可。

## 想留现场排查

`shutdownAfterJobFinishes: true` 会删掉整个 RayCluster。要保住现场：

```bash
# 临时改成 false，或者把 TTL 放大到够你登进去看
kubectl patch rayjob daft-audio-bench -n daft-bench \
  --type=merge -p '{"spec":{"ttlSecondsAfterFinished":3600}}'
```

注意 `spec` 的多数字段在 RayJob 运行中改不动，`ttlSecondsAfterFinished` 要在作业进入终态**之前**改。
更稳的做法是提前把 `shutdownAfterJobFinishes` 设成 `false`，代价是集群要手动删。

要经常这样排查，就别用临时集群 —— 换成 `40-raycluster.yaml` + `41-rayjob-existing.yaml`，
集群本来就不会被回收。

## 清理

```bash
# 临时集群路径：删 RayJob，operator 会把它建的集群一起带走
kubectl delete rayjob daft-audio-bench -n daft-bench --ignore-not-found
kubectl delete raycronjob daft-audio-nightly -n daft-bench --ignore-not-found

# 常驻集群路径：RayJob 和 RayCluster 要分别删，operator 不碰后者
kubectl delete rayjob daft-audio-run-001 -n daft-bench --ignore-not-found
kubectl delete raycluster daft-audio -n daft-bench --ignore-not-found

# 平台层（会连带删掉 MinIO 里的数据集和产物）
kubectl delete job bench-generate -n daft-bench --ignore-not-found
kubectl delete -f 05-daft-dashboard.yaml --ignore-not-found
kubectl delete -f 00-platform.yaml --ignore-not-found
```
