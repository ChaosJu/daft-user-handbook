# RayJob 实战

[上一页](deploy-kuberay.md)讲选型。这一页走一遍改造：把一套"常驻 RayCluster + 外部 submit Job"的真实压测，按官方 [RayJob Quickstart](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html) 的形态重新组织。

清单在 [`examples/kuberay/`](../../examples/kuberay/)。参数不是示意值：`10 × 2C/10Gi`、`object-store-memory 2Gi`、`/dev/shm 3Gi`、`asr-actor-concurrency 16`、`default_morsel_size 8` 都来自那次压测的实际配置。基线是 KubeRay v1.6.2 + Ray 2.55.1。

官方中文镜像：<https://docs.rayai.org.cn/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html>

!!! tip "手上还没有镜像？"
    先跑 [`examples/quickstart/`](../../examples/quickstart/)：两个文件、官方 `rayproject/ray` 镜像、官方示例脚本，零构建就能确认 KubeRay 和 RayJob 装对了。跑通再回来看这一页。详见本页末尾[官方镜像 + 官方示例](#官方镜像-官方示例)。

## RayJob 管两样东西

```text
RayJob CR
  ├─ RayCluster   来自 rayClusterSpec（新建）或 clusterSelector（复用已有）
  └─ Submitter    一个 K8s Job，跑 `ray job submit --address ... -- $entrypoint`
```

三个不要混的词：

| 词 | 是什么 |
|---|---|
| **RayJob** | KubeRay 的 CRD |
| **Ray job** | 提交到集群上的那份作业 |
| **Submitter** | 执行 `ray job submit` 的 K8s Job |

KubeRay 往 submitter Pod 注入两个环境变量：

```text
RAY_DASHBOARD_ADDRESS   $HEAD_SERVICE:$DASHBOARD_PORT
RAY_JOB_SUBMISSION_ID   这次 Ray job 的 submission id
```

提交器容器按**位置**识别，不是按名字——官方原话是 "the first container is assumed to be the submitter container"。所以 `initContainers` 可以随便加，但不能把别的业务容器排到它前面。也不要自己写它的 `command`：留空时 KubeRay 会用上面两个变量拼出提交命令，写死就等于放弃了 `entrypoint` 字段。

## 改造对照表

原来五个文件各管一段生命周期，改完是这样：

| 原文件 | 去处 |
|---|---|
| `00-platform.yaml` | 保留。加 `run-bench.sh` / `publish-artifacts.py`，`wait.py` 去掉 ray/dashboard 探针 |
| `05-daft-dashboard.yaml` | 保留。独立 Deployment 从"更好"变成"必须" |
| `20-generate-job.yaml` | → `10-generate-job.yaml`，只改编号 |
| `10-raycluster.yaml` | → `20-rayjob.yaml` 的 `spec.rayClusterSpec` |
| `30-submit-job.yaml` | → `20-rayjob.yaml` 的 `spec.entrypoint` + `spec.submitterPodTemplate` |

三个判断值得单独说。

**不是所有步骤都该做成 RayJob。** `10-generate-job.yaml` 保持普通 `batch/v1` Job：生成是单线程的（每个 clip 一次 ffmpeg + 一次上传），只依赖 MinIO，不需要 Ray。包成 RayJob 等于为一个单进程任务拉起整个集群。**只有真正需要分布式执行的步骤才值得改造。**

**手写的提交逻辑全部删掉。** 原来那 60 行内联 Python（`JobSubmissionClient` → `submit_job` → `tail_job_logs` → 检查终态）是 KubeRay 的内置行为，`submissionMode: K8sJobMode` 就够了。

**产物必须自己推走。** 这是改造引入的唯一新代码。`benchmark.audio run --run-dir` 只写本地目录，而 `shutdownAfterJobFinishes` 会删掉持有那个目录的 head Pod。所以 `entrypoint` 套了一层 `run-bench.sh`，在 driver 退出前把 `/out/<RUN_ID>` 推到 S3：

```bash
# 故意不用 set -e：benchmark 失败时也必须上传，
# 因为失败那次的 report 和 resources.csv 就是全部诊断依据
/opt/venv/bin/python -m benchmark.audio run ... "$@"
rc=$?
/opt/venv/bin/python /scripts/publish-artifacts.py "$RUN_DIR" "$ARTIFACTS_URI" || rc=1
exit "$rc"
```

## 两条路径

`20-rayjob.yaml` 和 `40-raycluster.yaml` + `41-rayjob-existing.yaml` 抢同一批节点，**不要同时 apply**。

| | 临时集群 | 常驻集群 |
|---|---|---|
| 文件 | `20-rayjob.yaml` | `40-raycluster.yaml` + `41-rayjob-existing.yaml` |
| 关键字段 | `spec.rayClusterSpec` | `spec.clusterSelector` |
| 每次启动 | 2–5 分钟拉集群 | 秒级，集群是热的 |
| 空闲成本 | 0 | 100Gi 一直占着 |
| 失败后 | 集群没了，只剩日志和 S3 产物 | 集群还在，能 `exec` 进 head 翻 `/tmp/ray` |
| 自动回收 | `shutdownAfterJobFinishes: true` | 不支持 |
| 适合 | 生产批处理、定时任务 | 连续调参、交互排查、严格独占节点做对比基准 |

两者**二选一**，KubeRay 的校验原话是 "one of RayClusterSpec or ClusterSelector must be set"。

## 步骤 0 · 装 Operator

```bash
# 联网（官方）
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm install kuberay-operator kuberay/kuberay-operator -n kuberay-system --create-namespace

# 离线：预渲染清单。必须 --server-side，CRD 超过 256KB 注解上限
kubectl apply --server-side -f kuberay-operator-v1.6.2.yaml
```

验收：

```bash
kubectl get crd | grep ray.io
# rayclusters.ray.io  rayjobs.ray.io  raycronjobs.ray.io  rayservices.ray.io

kubectl -n kuberay-system rollout status deploy/kuberay-operator
```

Operator 没起来时，RayJob 能创建，但**永远不产生 Pod**。先看 operator 日志，不要先调 Daft 参数。

## 步骤 1 · 改三处占位

```bash
# 1. MinIO 钉到有大容量 /data 的节点。hostPath 是节点本地数据，不能漂
sed -i 's/REPLACE_WITH_NODE_NAME/node-01/' examples/kuberay/00-platform.yaml

# 2. 镜像换成离线 registry，并放开 imagePullSecrets
sed -i 's|daft-audio:offline|registry.local/daft-audio:offline|g' examples/kuberay/*.yaml
```

`rayVersion` 必须和镜像里的 Ray 一致（这里是 **2.55.1**）。head、worker、submitter、dashboard 必须同一个不可变 tag，不要 `latest`。

第 3 处占位是 `REPLACE_WITH_DATA_RUN_ID`，得等下一步跑完才知道。

## 步骤 2 · 平台层与数据集

```bash
kubectl apply -f examples/kuberay/00-platform.yaml
kubectl apply -f examples/kuberay/05-daft-dashboard.yaml   # 可选

kubectl apply -f examples/kuberay/10-generate-job.yaml
kubectl logs -n daft-bench job/bench-generate -f
# 记下 GEN_RUN_ID，填进 20-rayjob.yaml 的 entrypoint
```

10k × 30s 的 clip 要 1–3 小时。同一份数据集复跑作业时跳过这步，`DATA_RUN_ID` 不变即可。

## 步骤 3 · apply RayJob（官方 Step 3）

```bash
kubectl apply -f examples/kuberay/20-rayjob.yaml
```

`entrypoint` 长这样，调优参数和它对应的集群规格放在同一个文件里，改集群就得同步改这几个数：

```text
/bin/bash /scripts/run-bench.sh <DATA_RUN_ID> auto
  --mock-url http://mock-llm:8010
  --ray-address auto        # driver 在 head 上，连本地集群
  --max-minutes 360
  --num-partitions 40       # 2 × 20 CPU
  --asr-actor-cpus 1
  --asr-actor-concurrency 16
  --asr-batch-size 1
  --llm-concurrency 16
```

ConfigMap 挂进来的 key 没有执行位，所以必须显式 `/bin/bash /scripts/...`。

集群真实预算是 **20 个 Ray CPU**（10 worker × `num-cpus 2`，head 是 0）。`1 × 16 = 16` 占掉其中 16 个，留 4 个给下载、LLM、Lance 写入。不要按 `10 × 8 = 80` 来配 actor。

## 步骤 4 · 核对状态（官方 Step 4）

```bash
kubectl get rayjob daft-audio-bench -n daft-bench -w
```

两个状态字段含义不同，都要看：

| 字段 | 谁的状态 | 成功值 |
|---|---|---|
| `jobStatus` | Ray job（作业进程） | `SUCCEEDED` |
| `jobDeploymentStatus` | KubeRay 对这次提交的部署 | `Complete` |

失败时 `.status.reason` 直接告诉你卡在哪一层：

```bash
kubectl get rayjob daft-audio-bench -n daft-bench \
  -o jsonpath='{.status.reason}{"\n"}{.status.message}{"\n"}'
```

| reason | 含义 |
|---|---|
| `PreRunningDeadlineExceeded` | 集群没能在 `preRunningDeadlineSeconds` 内进 Running |
| `SubmissionFailed` | 提交器 Pod 自己失败了 |
| `DeadlineExceeded` | 撞 `activeDeadlineSeconds` |
| `AppFailed` | 集群和提交都正常，是作业自己失败 |
| `ValidationFailed` | spec 非法，operator 根本没动手 |

`preRunningDeadlineSeconds: 1800` 是 RayJob 独有、也是排障提速最有效的开关：镜像冷拉取、worker Pending、探针不过都会撞它，而不是干等 6 小时的 `activeDeadlineSeconds`。常驻 RayCluster 没有这个概念。

确认 Ray 真的看到 20 个 CPU：

```bash
HEAD=$(kubectl get pod -n daft-bench -l ray.io/node-type=head -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n daft-bench -c ray-head "$HEAD" -- ray status
```

Total CPU 不是 20：先 `describe` Pending 的 worker，不要去加 partition。

## 步骤 5 · 看输出（官方 Step 5）

```bash
# driver 日志（跑在 head 上）
kubectl logs -n daft-bench -l ray.io/node-type=head -f

# 提交器日志：ray job submit 的输出，含最终判定
kubectl logs -n daft-bench -l job-name=daft-audio-bench -f
```

`kubectl logs` 看不见 `/tmp/ray`。UDF 里的报错要进 worker：

```bash
WORKER=$(kubectl get pod -n daft-bench -l ray.io/node-type=worker -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n daft-bench -c ray-worker "$WORKER" -- ls -lt /tmp/ray/session_latest/logs
```

Ray Dashboard 的集群名带随机后缀，从状态里取，不要写死：

```bash
CL=$(kubectl get rayjob daft-audio-bench -n daft-bench -o jsonpath='{.status.rayClusterName}')
kubectl -n daft-bench port-forward "svc/${CL}-head-svc" 8265:8265
```

Daft 自己的查询级观测在 `:3238`，和 Ray Dashboard 的 `:8265` 不是一个东西：

```bash
kubectl -n daft-bench port-forward svc/daft-dashboard 3238:3238
```

Daft Dashboard 做成独立 Deployment 而不是 head sidecar，在 RayJob 形态下是必须的——sidecar 会跟着集群一起被删，作业跑完就没 UI 可看了。

## 步骤 6 · 回收（官方 Step 7–9）

```yaml
shutdownAfterJobFinishes: true
ttlSecondsAfterFinished: 600    # 10 分钟后删 RayCluster
```

submitter Job 不会一起删。官方说明：它扛着 `ray job` 日志，且跑完不再占计算资源；删 RayJob CR 时才会被 ownerReference 带走。

产物在 S3，不在 Pod 里：

- 报表与采样 `s3://benchmark/audio/<RUN_ID>/artifacts/`（`report.md`、`resources.csv`）
- 输出数据 `s3://benchmark/audio/<RUN_ID>/output.lance`

想留现场排查：

```bash
# 要在作业进入终态之前改
kubectl patch rayjob daft-audio-bench -n daft-bench \
  --type=merge -p '{"spec":{"ttlSecondsAfterFinished":3600}}'
```

经常需要这样排查，就别用临时集群，换成下面的常驻路径。

## 步骤 7 · 再跑一次

RayJob 是一次性对象。对已 `Complete` 的 CR 再 `apply` **不会重跑**：

```bash
kubectl delete rayjob daft-audio-bench -n daft-bench --ignore-not-found
kubectl apply -f examples/kuberay/20-rayjob.yaml
```

并发或 CI 场景给每次一个唯一 `metadata.name`，或者 `generateName` + `kubectl create`。

## 路径 B · 常驻集群 + clusterSelector

官方的 "use existing RayCluster"，对应 `40-raycluster.yaml` + `41-rayjob-existing.yaml`。

```bash
# 先给刚好 10 个节点打标签（40 保留了硬性 nodeSelector + 反亲和 + DoNotSchedule）
for n in node-01 node-02 node-03 node-04 node-05 \
         node-06 node-07 node-08 node-09 node-10; do
  kubectl label node "$n" daft-bench/ray-worker=true --overwrite
done

kubectl apply -f examples/kuberay/40-raycluster.yaml
kubectl get raycluster daft-audio -n daft-bench -w      # 等 STATUS=ready

# 每轮调参：改 41 里的参数 + 换 metadata.name，再 apply
kubectl apply -f examples/kuberay/41-rayjob-existing.yaml
```

`clusterSelector` 的 key 固定是 `ray.io/cluster`，value 是 RayCluster 的 `metadata.name`。常驻集群名字固定，所以 head Service 也是固定的 `daft-audio-head-svc`——这是它相对临时集群的实际便利。

这个模式有四条硬约束，都是 KubeRay 的行为而非风格选择：

- **`shutdownAfterJobFinishes` 静默忽略。** controller 在作业进终态时先判 `clusterSelector` 就直接 `return`（注释原文 "we must not delete it"），既不生效也不报错。别指望它回收，常驻集群只能手动 `kubectl delete raycluster`。`ttlSecondsAfterFinished > 0` 又要求 `shutdownAfterJobFinishes=true`，所以两个字段一起省掉才合法。
- **`backoffLimit` 只能是 `0`**（"BackoffLimit is incompatible with ClusterSelector mode"）。作业级重试的语义是"新建一整个集群重跑"，这个模式下没有集群可建。
- **`suspend` 不支持**，所以这条路径没法交给 Kueue 排队调度。
- **`submissionMode` 不能是 `SidecarMode`。**

常驻集群里 worker Pending 是**想要的**信号：你会看到并处理，而不是让作业带着 7 个 worker 偷偷跑完，产出一份没法和上次对比的报表。`20-rayjob.yaml` 把这套放宽成 `ScheduleAnyway`，因为 RayJob 要等集群 ready 才提交，一个 Pod 排不下就会一路拖到 `preRunningDeadlineSeconds` 才失败。

不经过 RayJob CR 的等价提交（临时验证用）：

```bash
kubectl exec -n daft-bench -c ray-head "$HEAD" -- \
  ray job submit --address http://127.0.0.1:8265 \
  -- /bin/bash /scripts/run-bench.sh "$DATA_RUN_ID" auto --mock-url http://mock-llm:8010
```

## 定时回归 · RayCronJob

改成 RayJob 之后顺带拿到的能力：`RayCronJobSpec.jobTemplate` 的类型就是 `RayJobSpec`，`20-rayjob.yaml` 的 spec 原样贴进去就能定时跑。老形态做不到，得自己写 CronJob 去调 Jobs API。

```bash
kubectl get crd raycronjobs.ray.io          # v1.6 新增，先确认存在
kubectl apply -f examples/kuberay/30-raycronjob.yaml

kubectl patch raycronjob daft-audio-nightly -n daft-bench \
  --type=merge -p '{"spec":{"suspend":true}}'    # 临时停掉
```

`RayCronJobSpec` 只有 `schedule` / `jobTemplate` / `suspend` 三个字段，没有 `concurrencyPolicy` 和 `startingDeadlineSeconds`。

cron 安全的前提是 `run-bench.sh` 第二个参数传 `auto`，每次按 UTC 时间生成新 `RUN_ID`。写死 `RUN_ID` 会让定时任务每天覆盖同一份 Lance 和报表。

回归的规模由**数据集**决定，不是命令行——`benchmark.audio run` 没有 `--limit` 之类的截断参数。要跑小样本就单独生成一份小数据集（`GEN_COUNT=500`），把 `DATA_RUN_ID` 指过去。`30-raycronjob.yaml` 的集群也故意只有 2 个 worker：每晚回归要的是"还能跑通、吞吐没掉一个数量级"，不是满负载压测。

## 字段对照：官方 Quickstart vs 我们的值

| 字段 | 我们的值 | 为什么 |
|---|---|---|
| `entrypoint` | `/bin/bash /scripts/run-bench.sh ...` | 套一层脚本推产物到 S3；ConfigMap 的 key 没有执行位 |
| `runtimeEnvYAML` | **不设** | 镜像里 `submit.py` 在 Job runtime_env 传 `env_vars`，`pipeline.py` 又传给 `ray.init()`，Ray 2.55 判冲突。依赖已烤进镜像 |
| `submissionMode` | `K8sJobMode` | 官方默认，且只有它支持 `submitterPodTemplate`（我们要插 `wait-deps`） |
| `shutdownAfterJobFinishes` | `true` | 10 × 10Gi = 100Gi 空转太贵 |
| `ttlSecondsAfterFinished` | `600` | 够 `kubectl logs` 捞最后一段；`>0` 必须配 `shutdownAfterJobFinishes=true` |
| `activeDeadlineSeconds` | `21600` | 6h 硬超时，含集群拉起 |
| `preRunningDeadlineSeconds` | `1800` | 集群拉不齐时快速失败，别占着节点 |
| `backoffLimit` | `0` | 每次重试新建一整个集群并重跑，基准测试自动重跑只会污染报表 |
| `submitterConfig.backoffLimit` | `2` | **默认值就是 2 不是 0**。它只重试"提交"动作，用固定 submission id 重连已在跑的 job，不重跑作业。保留它，提交器 Pod 偶发被驱逐时不至于让 6h 作业作废 |
| `num-cpus`（head） | `"0"` | 官方套路：head 不接计算，ASR actor 不会落到 2 CPU 的 GCS Pod 上 |
| worker | `10 × 2C/10Gi` | 节点能排下的规格。KubeRay 按 **limits** 上报资源，requests 被忽略，所以 `request == limit` |

## 作业入口该长什么样

```python
import daft

daft.set_runner_ray()                          # driver 已在集群内，不用传 address
daft.set_execution_config(
    default_morsel_size=8,                     # 胖行的第一内存旋钮
    maintain_order=False,
)
df = daft.read_parquet(manifest_uri)
df = df.into_partitions(40).with_column(...)
df.write_lance(out_uri, mode="overwrite")      # 生产终点是写出，不是 collect
```

不要：`collect()`、`to_pandas()`、`ray://` Client、`set_runner_native()`。理由见[生产禁区](production-donts.md)。

`default_morsel_size` 只能通过 `set_execution_config` 传——Daft 自己不读 `DAFT_DEFAULT_MORSEL_SIZE` 环境变量。`00-platform.yaml` 里设了这个变量却能生效，是因为**应用代码**（`config.py` → `pipeline.py`）把它读出来再显式传进 `set_execution_config`。细节见 [Morsel](../02-principles/morsel.md)。

## 三十秒体检

作业还在跑的时候：

```bash
kubectl exec -n daft-bench -c ray-head "$HEAD" -- ray status
kubectl get pods -n daft-bench -l ray.io/node-type=worker
kubectl exec -n daft-bench -c ray-head "$HEAD" -- ray list actors --address http://127.0.0.1:8265
```

```text
Total CPU < 20     worker 没齐，先查 Pending，不要动 Daft 参数
RESTARTS 在涨      内存问题，先留现场
actor RESTARTING   等下去没有意义
```

完整排障见[如何看日志](../04-observability/logs.md)和[根据监控调参](../04-observability/tune-from-metrics.md)。

## 官方镜像 + 官方示例

[`examples/quickstart/`](../../examples/quickstart/) 是同一种形态（RayJob + 临时集群）的最小版本，只有两个文件：

```bash
kubectl apply -f examples/quickstart/00-configmap-script.yaml
kubectl apply -f examples/quickstart/10-rayjob.yaml
kubectl logs -n daft-quickstart -l job-name=daft-quickstart -f
```

**先纠正一个常见误解：Daft 没有官方镜像。** 官方 Helm chart（Daft 仓库 `k8s/charts/quickstart`）的默认镜像是 Ray 官方的 `rayproject/ray:2.46.0-py312-cpu`，Daft 是运行时装进去的——driver 靠 `uv run --script` 读 PEP 723 内联依赖，worker 靠 `ray.init(runtime_env={"pip": ["daft"]})`。`uv` 这条路能走通是因为 Ray 2.45 起官方镜像自带它。

所以有三处版本必须对齐，否则 driver 连不上 GCS（`uv` 建的 venv 是隔离的，不复用镜像里的 ray）：镜像 tag、`rayVersion`、脚本里的 `ray[client]==2.46.0`。

示例脚本是官方文档的原文，一字不改，包括 `print(df.collect())`。这样照抄的代价是它同时踩了本手册三条红线，而这正好是最好的对照材料：

| 官方 quickstart 怎么写 | 生产该怎么写 | 为什么 |
|---|---|---|
| `print(df.collect())` | `df.write_parquet(...)` | `collect()` 把整个结果拉回 driver |
| `uv run` + `runtime_env={"pip": [...]}` | 依赖烤进镜像 | 每个 worker 都要连 PyPI；离线不可用；driver/worker 版本无保证 |
| `dependencies = ["daft"]` | `daft==0.7.x` | 不锁版本，今天和下周跑的不是同一个引擎 |

反过来，改成 RayJob 之后有一处比官方 chart 更对：官方 chart 的 job 模板设 `RAY_ADDRESS=ray://<head>:10001`，走的是 [Ray Client](production-donts.md)——driver 在集群外，靠一条脆弱的长连接代理。RayJob 的 driver 由 Jobs API 拉起、跑在 head 上，`ray.init()` 直连本地 GCS，这条网络路径根本不存在。

官方 chart 本身不用 KubeRay（"No operator required: Uses native Kubernetes resources only"，head/worker 是 `Deployment`，作业是 `batch/v1` Job）。想先跑原版：

```bash
helm install my-job oci://ghcr.io/eventual-inc/daft/quickstart \
  --set distributed=true --set worker.replicas=3 \
  --set-file job.script=main.py
```

`quickstart` 那套**离线跑不了**（依赖是运行时装的）。内网环境直接用 `examples/kuberay/`。

## 清理

```bash
# quickstart 那套
kubectl delete rayjob daft-quickstart -n daft-quickstart --ignore-not-found

# 临时集群路径：删 RayJob，operator 把它建的集群一起带走
kubectl delete rayjob daft-audio-bench -n daft-bench --ignore-not-found
kubectl delete raycronjob daft-audio-nightly -n daft-bench --ignore-not-found

# 常驻集群路径：两个对象分别删，operator 不碰 RayCluster
kubectl delete rayjob daft-audio-run-001 -n daft-bench --ignore-not-found
kubectl delete raycluster daft-audio -n daft-bench --ignore-not-found

# 平台层（会连带删掉 MinIO 里的数据集和产物）
kubectl delete job bench-generate -n daft-bench --ignore-not-found
kubectl delete -f examples/kuberay/05-daft-dashboard.yaml --ignore-not-found
kubectl delete -f examples/kuberay/00-platform.yaml --ignore-not-found
```
