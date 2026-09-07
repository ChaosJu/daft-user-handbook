# KubeRay 生产示例

在 [`../quickstart/`](../quickstart/) 跑通链路之后再用本目录。部署概念与 RayJob 字段说明见手册：[KubeRay RayJob 部署](../../docs/03-deploy-kuberay.md)。

环境基线：KubeRay v1.6.2、Ray 2.55.1、namespace `daft-bench`。

## 文件清单

| 文件 | 作用 |
|---|---|
| `00-platform.yaml` | namespace、Secret、MinIO、Mock LLM、`bench-env` / `bench-scripts` |
| `05-daft-dashboard.yaml` | 可选，Daft Dashboard `:3238` |
| `10-generate-job.yaml` | 生成输入数据（普通 Job，**不是** RayJob） |
| **`20-rayjob.yaml`** | **生产默认**：RayJob + 临时集群 |
| `30-raycronjob.yaml` | 可选，定时回归 |
| `40-raycluster.yaml` + `41-rayjob-existing.yaml` | 可选，调参：`clusterSelector` 打进常驻集群 |

`20` 与 `40`+`41` **不要同时 apply**。

## 快速开始

apply 前改三处：

1. `00-platform.yaml` 里 MinIO 的 `nodeSelector`（钉到有 `/data` 的节点）
2. 镜像 registry（离线环境）
3. `20-rayjob.yaml` 的 `entrypoint` 里 `REPLACE_WITH_DATA_RUN_ID`

```bash
kubectl apply -f 00-platform.yaml
kubectl apply -f 10-generate-job.yaml
kubectl logs -n daft-bench job/bench-generate -f    # 记下 GEN_RUN_ID

kubectl apply -f 20-rayjob.yaml
kubectl get rayjob daft-audio-bench -n daft-bench -w
kubectl logs -n daft-bench -l job-name=daft-audio-bench -f
```

## 和 Quickstart 的三处差异

| | Quickstart | 本目录 `20-rayjob.yaml` |
|---|---|---|
| 依赖 | 镜像里已烤好 | 同上 + MinIO / mock-llm 要探活 |
| Submitter | KubeRay 默认 | 自定义 `submitterPodTemplate` 插 `wait-deps` |
| 产物 | 无（示例 `collect()` 六行） | `run-bench.sh` 推 S3，因为 `shutdownAfterJobFinishes` 会删 head |

**不是所有步骤都该做成 RayJob。** `10-generate-job.yaml` 是单线程 ffmpeg + 上传，只依赖 MinIO，包成 RayJob 等于为一个单进程任务拉起整个集群。

**产物必须自己推走。** `run-bench.sh` 在 driver 退出前把 `/out/<RUN_ID>` 推到 S3——集群回收后 Pod 里什么都不剩。

## 字段对照（相对官方 Quickstart）

| 字段 | 我们的值 | 为什么 |
|---|---|---|
| `entrypoint` | `/bin/bash /scripts/run-bench.sh ...` | 套脚本推产物；ConfigMap key 无执行位 |
| `runtimeEnvYAML` | **不设** | 依赖已烤进镜像；设了和 `ray.init()` 传 env 会冲突 |
| `shutdownAfterJobFinishes` | `true` | 空闲 100Gi 太贵 |
| `ttlSecondsAfterFinished` | `600` | 够捞日志；>0 必须配 shutdown |
| `preRunningDeadlineSeconds` | `1800` | 集群拉不齐快速失败，别干等 6h |
| `activeDeadlineSeconds` | `21600` | 6h 硬超时 |
| `backoffLimit` | `0` | 重试 = 新建整集群，污染基准 |
| `submitterConfig.backoffLimit` | `2` | 只重试提交动作，不重跑作业 |
| head `num-cpus` | `"0"` | head 不接计算 |
| worker | `10 × 2C/10Gi` | 按节点能排下的规格；`request == limit` |

## 路径 B · 常驻集群

调参、留 `/tmp/ray` 现场时用 `40-raycluster.yaml` + `41-rayjob-existing.yaml`：

```bash
kubectl apply -f 40-raycluster.yaml
kubectl get raycluster daft-audio -n daft-bench -w

kubectl apply -f 41-rayjob-existing.yaml   # 每轮换 metadata.name + 参数
```

硬约束：

- `shutdownAfterJobFinishes` **静默忽略**，集群只能手动删
- `backoffLimit` 只能是 `0`
- `submissionMode` 不能是 `SidecarMode`

## 定时回归 · RayCronJob

```bash
kubectl apply -f 30-raycronjob.yaml
```

`jobTemplate` 的类型就是 `RayJobSpec`，`20-rayjob.yaml` 的 spec 可原样贴入。cron 安全前提是 `run-bench.sh` 第二个参数传 `auto` 生成新 `RUN_ID`。

## 产物与清理

- 报表 `s3://benchmark/audio/<RUN_ID>/artifacts/`
- 输出 `s3://benchmark/audio/<RUN_ID>/output.lance`

```bash
kubectl delete rayjob daft-audio-bench -n daft-bench --ignore-not-found
kubectl delete raycluster daft-audio -n daft-bench --ignore-not-found   # 路径 B
kubectl delete -f 00-platform.yaml --ignore-not-found
```
