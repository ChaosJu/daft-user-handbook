# KubeRay RayJob 示例

可 apply 的 YAML 在这目录。**部署步骤、字段说明、排障、两条路径的对照**见手册：[KubeRay RayJob 部署](../../docs/03-deploy-kuberay.md)。

环境基线：KubeRay v1.6.2、Ray 2.55.1、镜像 `daft-audio:offline`（或 [`../docker/`](../docker/) 的 `daft-ray-ops`）、namespace `daft-bench`。

> 还没装 KubeRay？先跑 [`../quickstart/`](../quickstart/) 验证链路。

## 文件清单

| 文件 | 作用 |
| --- | --- |
| `00-platform.yaml` | namespace、Secret、MinIO、Mock LLM、`bench-env` / `bench-scripts` |
| `05-daft-dashboard.yaml` | 可选，Daft Dashboard `:3238` |
| `10-generate-job.yaml` | 生成输入数据（普通 Job，**不是** RayJob） |
| **`20-rayjob.yaml`** | **生产默认**：RayJob + 临时集群 |
| `30-raycronjob.yaml` | 可选，定时回归 |
| `40-raycluster.yaml` + `41-rayjob-existing.yaml` | 可选，调参：`clusterSelector` 打进常驻集群 |

`20` 与 `40`+`41` **不要同时 apply**。

## 快速开始

apply 前改三处：`00-platform.yaml` 的 MinIO `nodeSelector`、镜像 registry、`entrypoint` 里的 `REPLACE_WITH_DATA_RUN_ID`。

```bash
kubectl apply -f 00-platform.yaml
kubectl apply -f 10-generate-job.yaml
kubectl logs -n daft-bench job/bench-generate -f    # 记下 GEN_RUN_ID，填进 20-rayjob.yaml

kubectl apply -f 20-rayjob.yaml
kubectl get rayjob daft-audio-bench -n daft-bench -w
kubectl logs -n daft-bench -l job-name=daft-audio-bench -f
```

其余（常驻集群路径、观察、产物、重跑、清理）见 [手册](../../docs/03-deploy-kuberay.md)。
