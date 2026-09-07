# Quickstart：最小可跑的 Daft on Ray

三个文件，用途是**验证 KubeRay + RayJob + Daft 这条链路通不通**：不用 MinIO、不用给节点打标签、不用改占位符。跑通了再去看生产参考 [`../kuberay/`](../kuberay/)。

| 文件 | 作用 |
| --- | --- |
| `00-configmap-script.yaml` | namespace + 示例脚本（`where` + `sort` 六行数据） |
| `10-rayjob.yaml` | RayJob + 临时集群，正式资源（head 2Gi / worker 2C·4Gi） |
| `10-rayjob-smoke.yaml` | 同上，缩到 4GB / 2CPU 虚拟机能跑 |

镜像是 [`../docker/`](../docker/) 构建的 `daft-ray-ops:2.55.1`，Daft 已烤进去，**运行期不装任何依赖，离线可用**。

## 跑一遍

```bash
# 1. 构建镜像（一次就够）
cd ../docker && bash build.sh
bash import-to-k3s.sh daft-ray-ops:2.55.1     # k3s 节点才需要，见下

# 2. apply
cd ../quickstart
kubectl apply -f 00-configmap-script.yaml
kubectl apply -f 10-rayjob.yaml                # 4GB 虚拟机换 10-rayjob-smoke.yaml

# 3. 看结果
kubectl get rayjob daft-quickstart -n daft-quickstart -w
kubectl logs -n daft-quickstart -l job-name=daft-quickstart -f
```

期望在日志里看到 `b` 为 true 的三行，按 `a` 排序：

```text
╭───────┬──────╮
│ a     ┆ b    │
╞═══════╪══════╡
│ 1     ┆ true │
│ 3     ┆ true │
│ 6     ┆ true │
╰───────┴──────╯
```

`jobStatus=SUCCEEDED` 且 `deploymentStatus=Complete` 才算通过，两个字段含义不同，见 [KubeRay RayJob 部署](../../docs/03-deploy-kuberay.md)。

RayJob 是一次性对象，改了 spec 必须**先删再建**：

```bash
kubectl delete rayjob daft-quickstart -n daft-quickstart --ignore-not-found
kubectl apply -f 10-rayjob.yaml
```

## Daft 没有官方镜像

这是最容易误解的一点。Daft 官方 Helm chart（`k8s/charts/quickstart`）的默认镜像是 **Ray 官方的 `rayproject/ray`**，Daft 是运行时装进去的：driver 靠 `uv run --script` 读 PEP 723 内联依赖，worker 靠 `ray.init(runtime_env={"pip": ["daft"]})`。

这套能跑，但有三个代价，所以这里不用它：

| 官方 chart 的做法 | 代价 |
| --- | --- |
| `uv` + `runtime_env={"pip": [...]}` | 每次作业、每个 worker 都要连 PyPI；**离线完全不可用** |
| `dependencies = ["daft"]` 不锁版本 | 今天跑和下周跑不是同一个引擎 |
| `RAY_ADDRESS=ray://<head>:10001` | 走 [Ray Client](../../docs/09-production-donts.md)，driver 在集群外，长连接一断作业就死 |

改成「依赖烤进镜像 + RayJob」之后三个问题一起消失：依赖是不可变制品，driver 由 Jobs API 拉起、跑在 head 上，`ray.init()` 直连本地 GCS。

想先跑官方原版（不装 KubeRay，head/worker 是 Deployment）：

```bash
helm install my-job oci://ghcr.io/eventual-inc/daft/quickstart \
  --set distributed=true --set worker.replicas=3 \
  --set-file job.script=main.py
```

## 示例脚本踩了一条生产红线

`main.py` 结尾是 `print(df.collect())`。示例只有六行数据，看得见输出才好验证链路；但 `collect()` 会把整个结果拉回 driver，**生产必须以 `write_*` 收尾**：

```python
df.write_parquet("s3://bucket/out", write_mode="overwrite")
```

完整清单见[生产禁区](../../docs/09-production-donts.md)。

## k3s 上的两个坑

在单机 k3s（3.8Gi RAM / 2 CPU）上实测过，遇到两个和 Daft 无关、但会让人误判的问题。

**1. Docker 里有镜像 ≠ k3s 能用。** k3s 用自己的 containerd，必须显式导入，否则 Pod 卡在 `FailedCreatePodSandBox`。沙箱依赖的 `rancher/mirrored-pause` 也一样：

```bash
docker save daft-ray-ops:2.55.1 | sudo k3s ctr -n k8s.io images import -

docker pull rancher/mirrored-pause:3.10.2 \
  || { docker pull registry.k8s.io/pause:3.10 \
       && docker tag registry.k8s.io/pause:3.10 rancher/mirrored-pause:3.10.2; }
docker save rancher/mirrored-pause:3.10.2 | sudo k3s ctr -n k8s.io images import -
```

**2. head 内存太小时 Ray 直接退出，不是 Daft 的问题。** 报错是 `minimum allowed is 78643200 bytes` —— Ray 按可用内存算 object store，算出来低于 75Mi 就拒绝启动。head 给 768Mi 会 CrashLoop。`10-rayjob-smoke.yaml` 里显式写死了这个下限，并手动限制 `/dev/shm`（KubeRay 默认按 memory limit 给，在小节点上会把 head 挤爆）。

小内存节点上集群拉起要几分钟、head 可能重启几次，所以 smoke 版把 `activeDeadlineSeconds` 放到 900s、`readinessProbe` 延到 60s。**超时值配小了会在集群就绪前就判失败**，看到 `reason=DeadlineExceeded` 先查这里。

## 清理

```bash
kubectl delete rayjob daft-quickstart -n daft-quickstart --ignore-not-found
kubectl delete -f 00-configmap-script.yaml --ignore-not-found
```
