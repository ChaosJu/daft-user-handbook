# 官方镜像 + 官方示例（零构建）

这套清单只有两个文件，用途是**在 Kind / 笔记本上验证 KubeRay + RayJob + Daft 这条链路通不通**：不用先建镜像、不用 MinIO、不用给节点打标签。

- 镜像：`rayproject/ray:2.46.0-py312-cpu`，官方 Daft Helm chart 的默认值
- 业务：[官方文档](https://docs.daft.ai/en/stable/distributed/kubernetes/) "Running on Kubernetes → Distributed Mode" 的示例脚本，**一字不改**
- 形态：RayJob + 临时集群，和 [`../kuberay/`](../kuberay/) 一致

```bash
kubectl apply -f 00-configmap-script.yaml
kubectl apply -f 10-rayjob.yaml

kubectl get rayjob daft-quickstart -n daft-quickstart -w
kubectl logs -n daft-quickstart -l job-name=daft-quickstart -f
```

作业本身是秒级的，时间几乎全花在装 `daft` 上。**离线环境跑不了这一份**，因为依赖是运行时装的——离线走 [`../kuberay/`](../kuberay/)。

## 先说清楚：Daft 没有官方镜像

这是最容易误解的一点。官方 Helm chart（Daft 仓库 `k8s/charts/quickstart`）的默认镜像是 **Ray 官方的 `rayproject/ray:2.46.0-py312-cpu`**，Daft 是运行时装进去的：

| 环节 | Daft 从哪来 |
| --- | --- |
| driver | `uv run --script` 读 `main.py` 开头的 PEP 723 块，临时建 venv |
| worker | `ray.init(runtime_env={"pip": ["daft"]})` |

`uv run` 这条路能走通，前提是 Ray 2.45 起 `rayproject/ray` 镜像自带 `uv`（[Ray 官方 uv 指南](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/uv.html)原文：*Starting with Ray 2.45, the `rayproject/ray:2.45.0` image includes `uv` as one of its dependencies*）。更早的 tag 里没有 `uv`，这套会直接失败。

**三处版本必须对齐**，否则 driver 连不上 GCS（uv 建的 venv 是隔离的，不复用镜像里的 ray）：

```text
10-rayjob.yaml            image: rayproject/ray:2.46.0-py312-cpu
10-rayjob.yaml            rayVersion: "2.46.0"
00-configmap-script.yaml  dependencies = [..., "ray[client]==2.46.0"]
```

## 三处和《生产禁区》冲突的地方

官方 quickstart 定位是"试驾"（chart 版本还是 `0.1.0`），不是生产形态。逐字照抄的代价就是它同时踩了手册里三条红线。**这正是保留这套示例的价值：对照着看，你能一眼看出生产该改什么。**

**1. `print(df.collect())`** —— `collect()` 把整个结果拉回 driver。示例只有 6 行无所谓，生产数据集会直接把 driver 撑爆。生产必须以 `write_*` 收尾：

```python
# quickstart
print(df.collect())

# 生产
df.write_parquet("s3://bucket/out", write_mode="overwrite")
```

**2. 运行时装依赖** —— `uv run` + `runtime_env={"pip": ["daft"]}` 意味着每次作业启动、每个 worker 都要连 PyPI 装一遍 daft。后果是启动慢、离线环境完全不可用、而且 driver 和 worker 装的版本没有任何东西保证一致。生产要把依赖烤进镜像，`../kuberay/` 那套就是这么做的。

**3. `dependencies = ["daft"]` 没锁版本** —— 你今天跑和下周跑拿到的是不同版本的 Daft。基准测试和生产都要锁死（`daft==0.7.x`），并且和镜像一起当成不可变制品。

## 一处我们比官方 chart 做得对

官方 chart 的 job 模板里设的是：

```yaml
- name: RAY_ADDRESS
  value: "ray://{{ head-svc }}:10001"
```

`ray://` 是 **Ray Client**，手册的[生产禁区](../../docs/03-best-practices/production-donts.md)明确反对——它是个脆弱的长连接代理，driver 在集群外，网络抖动就断，且不走 Jobs API 那套重试和日志。

改成 RayJob 之后这个问题自动没了：driver 由 Ray Jobs API 拉起、**跑在 head 上**，`ray.init()` 直接连本地 GCS，不需要 `RAY_ADDRESS`，也不需要 `ray[client]` 那条网络路径（脚本里那个依赖只是照抄官方保留的，实际没用到 Client 协议）。

## 和官方 chart 的其他差异

官方 chart 明确说 "No operator required: Uses native Kubernetes resources only"，所以它和我们的形态是两条路：

| | 官方 chart | 这套 |
| --- | --- | --- |
| 编排 | 纯原生资源：head/worker 是 `Deployment`，作业是 `batch/v1` Job | RayJob CRD |
| 谁管集群生命周期 | `helm install` / `helm uninstall` | KubeRay，`shutdownAfterJobFinishes` |
| driver 位置 | 独立的 job Pod，经 `ray://` 连进来 | head 上，Jobs API 拉起 |
| 等集群就绪 | job 的 initContainer 循环 `ray health-check` | KubeRay 内置，外加 `preRunningDeadlineSeconds` |
| 失败原因 | 自己看 Pod 事件 | `.status.reason`（`PreRunningDeadlineExceeded` / `AppFailed` / …） |
| 监控 | Prometheus + Grafana 作为 head 的 sidecar，默认开 | 交给集群里已有的 Prometheus，见[如何看监控](../../docs/04-observability/monitoring.md) |

想先跑官方原版（不装 KubeRay）也很快：

```bash
helm install my-job oci://ghcr.io/eventual-inc/daft/quickstart \
  --set distributed=true \
  --set worker.replicas=3 \
  --set-file job.script=main.py

kubectl logs -f job/my-job-quickstart-job
helm uninstall my-job
```

## 自建镜像（推荐生产形态）

[`../docker/`](../docker/) 提供通用 **`daft-ray-ops`** 镜像：Daft + 常用 I/O 依赖烤进镜像，附带 `curl` / `jq` / `mc` / `aws` 等运维 CLI。比 `daft-audio:offline` 轻，比运行时装 pip 快且可离线。

```bash
cd ../docker && bash build.sh
bash import-to-k3s.sh daft-ray-ops:2.55.1   # k3s 节点

kubectl apply -f 00-configmap-script-baked.yaml
kubectl apply -f 10-rayjob-baked-smoke.yaml   # 4GB VM
# 或 10-rayjob-baked.yaml（正式资源）
```

baked 版脚本用 `daft.set_runner_ray()`，**无** PEP 723、**无** `runtime_env={"pip": ["daft"]}`。

## 三套示例怎么选

| 你要做什么 | 用哪套 |
| --- | --- |
| 验证 KubeRay / RayJob 装对了没 | 这套（官方镜像 + `10-rayjob-smoke.yaml`） |
| 第一次接触 Daft on Ray，想看最小可跑形态 | 这套 |
| 通用 Daft 流水线，依赖烤进镜像 | [`../docker/`](../docker/) + `10-rayjob-baked*.yaml` |
| 离线 / 内网环境 | [`../docker/`](../docker/) 或 [`../kuberay/`](../kuberay/) |
| 真实数据、要看内存和吞吐 | [`../kuberay/`](../kuberay/) |
| 上生产 | [`../docker/`](../docker/) 或 [`../kuberay/`](../kuberay/)，并过一遍[生产禁区](../../docs/03-best-practices/production-donts.md) |

配套讲解见 [RayJob 实战](../../docs/03-best-practices/rayjob-hands-on.md)。

## daftvm / k3s 踩坑（已在 192.168.138.131 验证）

### pause 镜像（`FailedCreatePodSandBox`）

k3s 沙箱依赖 `rancher/mirrored-pause:3.10.2`。Docker Hub 连不上时，必须导入到 **k3s containerd**（Docker 里有 ≠ k3s 能用）：

```bash
docker pull rancher/mirrored-pause:3.10.2 \
  || { docker pull registry.k8s.io/pause:3.10 && docker tag registry.k8s.io/pause:3.10 rancher/mirrored-pause:3.10.2; }
docker save rancher/mirrored-pause:3.10.2 | sudo k3s ctr -n k8s.io images import -
```

一键：`bash fix-pause-and-test.sh`（会写 registries 镜像加速、装 KubeRay、跑 smoke）。

### head 内存 CrashLoop

head 只给 768Mi 时 Ray 报错 `minimum allowed is 78643200 bytes` 并退出。用 `10-rayjob-smoke.yaml`（head 2Gi + 显式 `object-store-memory: "78643200"`），不要用 `10-rayjob.yaml`。

### 实测结果

```text
jobStatus=SUCCEEDED  deploymentStatus=Complete  ~79s
```

辅助脚本：`run-on-daftvm.sh`（从零装 k3s）、`fix-pause-and-test.sh`、`retry-rayjob.sh`（只重跑 RayJob）。

## 清理

```bash
kubectl delete rayjob daft-quickstart -n daft-quickstart --ignore-not-found
kubectl delete -f 00-configmap-script.yaml --ignore-not-found
```
