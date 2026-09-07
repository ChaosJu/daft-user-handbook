# =============================================================================
# Daft + Ray 运维镜像 —— 依赖烤进镜像，运行期零 pip
#
# 用途：
#   - KubeRay RayJob / RayCluster 的 head & worker 统一镜像
#   - 离线 / 内网环境（构建期联网一次，运行期不需要 PyPI）
#   - 排障：curl / jq / mc / aws / vim / top / ps / netstat 等常见运维工具
#
# 构建（有网机器）：
#   docker build -t daft-ray-ops:2.55.1 .
#   # 或指定 Ray 版本（须与 rayVersion、KubeRay 对齐）：
#   docker build --build-arg RAY_VERSION=2.46.0 -t daft-ray-ops:2.46.0 .
#
# 离线分发：
#   docker save daft-ray-ops:2.55.1 | gzip > daft-ray-ops-2.55.1.tar.gz
#   # k3s 节点：gunzip -c ... | sudo k3s ctr -n k8s.io images import -
# =============================================================================

ARG RAY_VERSION=2.55.1
FROM rayproject/ray:${RAY_VERSION}-py312-cpu

USER root

# -----------------------------------------------------------------------------
# 1) 运维常用 CLI（Ray 基础镜像是 Ubuntu，以 root 装包后切回 ray 用户）
# -----------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        wget \
        jq \
        vim \
        less \
        procps \
        net-tools \
        iproute2 \
        netcat-openbsd \
        dnsutils \
        lsof \
        unzip \
        git \
    && rm -rf /var/lib/apt/lists/*

# procps → top, ps, free, uptime；net-tools → netstat, ifconfig
RUN command -v top && command -v ps && command -v netstat && command -v ss

# MinIO client —— 与 kuberay 示例里的 S3/MinIO 排障配套
RUN curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc \
        -o /usr/local/bin/mc \
    && chmod +x /usr/local/bin/mc

# -----------------------------------------------------------------------------
# 2) Python 依赖（Daft + 常用 I/O extra，不装 daft[ray]）
# -----------------------------------------------------------------------------
ENV PIP_NO_CACHE_DIR=1
COPY requirements.txt /tmp/requirements.txt
RUN /home/ray/anaconda3/bin/pip install -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# -----------------------------------------------------------------------------
# 3) 内置冒烟脚本（也可被 RayJob entrypoint 直接引用）
# -----------------------------------------------------------------------------
COPY scripts/smoke.py /opt/daft-handbook/smoke.py
RUN chown ray:users /opt/daft-handbook/smoke.py

# 构建期自检：Daft 能 import，且 Ray 版本未被意外覆盖
RUN /home/ray/anaconda3/bin/python -c "import daft, ray; print('daft', daft.__version__); print('ray', ray.__version__)"

USER ray
WORKDIR /home/ray
ENV PATH="/home/ray/anaconda3/bin:${PATH}"
