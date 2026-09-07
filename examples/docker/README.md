# Daft + Ray 运维镜像（`daft-ray-ops`）

在官方 `rayproject/ray` 基础上预装 **Daft 及常用 I/O 依赖**，并补齐 K8s 排障常用 CLI。构建一次后，RayJob 运行期**不再需要** `uv run` 或 `runtime_env={"pip": ["daft"]}`。

## 镜像里有什么

| 类别 | 内容 |
| --- | --- |
| 基础 | `rayproject/ray:<RAY_VERSION>-py312-cpu`（默认 **2.55.1**，与 [`../kuberay/`](../kuberay/) 对齐） |
| Daft | `daft[aws,lance,pandas]==0.7.24`（**不含** `daft[ray]`，避免覆盖镜像内 Ray） |
| I/O / 云 | `s3fs`、`awscli`（MinIO / S3 排障） |
| 运维 CLI | `curl` `wget` `jq` `vim` `less` `top` `ps` `netstat` `ss` `procps` `iproute2` `nc` `dig` `lsof` `git` |
| 对象存储 | MinIO Client `mc`（`/usr/local/bin/mc`） |
| 冒烟脚本 | `/opt/daft-handbook/smoke.py` |

与 [`../kuberay/`](../kuberay/) 里的 `daft-audio:offline` 相比：本镜像**不含** ASR 模型、FunASR、ffmpeg 等业务栈，体积更小，适合通用 Daft 流水线。

## 构建

```bash
cd examples/docker

# 默认 Ray 2.55.1，与 ../quickstart/ 和 ../kuberay/ 的 rayVersion 一致
bash build.sh

# 换 Ray 版本时，示例 YAML 的 image tag 和 rayVersion 要同步改
RAY_VERSION=2.46.0 IMAGE=daft-ray-ops:2.46.0 bash build.sh
```

Windows 直接：

```powershell
cd examples\docker
docker build --build-arg RAY_VERSION=2.55.1 -t daft-ray-ops:2.55.1 .
```

本地冒烟（不启动 Ray 集群，只验证 import）：

```bash
docker run --rm daft-ray-ops:2.55.1 python -c "import daft, ray; print(daft.__version__, ray.__version__)"
```

## 离线 / k3s 导入

Docker 里有的镜像，k3s containerd **不一定**能用到，需显式导入：

```bash
# 导出（在有网机器）
docker save daft-ray-ops:2.55.1 | gzip > daft-ray-ops-2.55.1.tar.gz

# 在 k3s 节点上导入
gunzip -c daft-ray-ops-2.55.1.tar.gz | sudo k3s ctr -n k8s.io images import -
# 本机构建的话直接：
bash import-to-k3s.sh daft-ray-ops:2.55.1
```

## 在 RayJob 里使用

两处版本必须对齐，否则 worker 连不上 head：

```text
build.sh          RAY_VERSION=2.55.1
10-rayjob.yaml    image: daft-ray-ops:2.55.1  +  rayVersion: "2.55.1"
```

跑一遍 [`../quickstart/`](../quickstart/) 验证：

```bash
kubectl apply -f ../quickstart/00-configmap-script.yaml
kubectl apply -f ../quickstart/10-rayjob.yaml       # 4GB 虚拟机换 10-rayjob-smoke.yaml
kubectl get rayjob daft-quickstart -n daft-quickstart -w
```

镜像里自带 `/opt/daft-handbook/smoke.py`，不想挂 ConfigMap 时可以直接：

```yaml
entrypoint: python /opt/daft-handbook/smoke.py
```

## 定制依赖

改 [`requirements.txt`](requirements.txt) 后重新 build。生产建议：

1. **锁版本**（已 pin `daft==0.7.24`）
2. **不要用 `daft[ray]`** —— 基镜像已有 Ray，extra 会 pip 覆盖
3. 需要 GPU / CUDA 时换 `rayproject/ray:<ver>-py312-gpu` 基镜像并自行验证 Daft wheel

## 两套镜像怎么选

| 镜像 | 何时用 |
| --- | --- |
| **`daft-ray-ops:<ray>`**（本目录） | 通用 Daft 流水线，依赖烤进镜像，带运维 CLI |
| `daft-audio:offline` | 完整音频 ASR benchmark，额外含 FunASR 模型与 ffmpeg（[`../kuberay/`](../kuberay/)） |
