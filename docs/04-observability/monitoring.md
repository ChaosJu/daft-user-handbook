# 如何看监控

四层指标互不覆盖。Ray 的 task 状态进不了 Loki，Daft 的查询计划进不了 Prometheus，Dashboard 重启就丢历史。

## K8s：判断 Pod 死活的唯一权威

cAdvisor（kubelet `/metrics/cadvisor`）：

| 指标 | 用途 |
|---|---|
| `container_memory_working_set_bytes` | **判断 Pod 是否触顶的唯一口径** |
| `container_spec_memory_limit_bytes` | 分母 |
| `container_cpu_cfs_throttled_seconds_total` | CPU 被限流 = `num-cpus` 与 limit 不一致 |
| `container_fs_usage_bytes` | 本地盘（spill、flight shuffle） |
| `container_network_receive_bytes_total` | 网络吞吐 |

kube-state-metrics：

| 指标 | 用途 |
|---|---|
| `kube_pod_container_status_restarts_total` | 重启次数上升 = actor / 进程被反复杀 |
| `kube_pod_container_status_last_terminated_reason` | `reason="OOMKilled"` |
| `kube_job_status_failed` / `_succeeded` | submit Job 终态 |
| `kube_pod_status_phase` | Pending 堆积 |

cAdvisor 回答“用了多少”，kube-state-metrics 回答“发生了什么”。只有后者能告诉你 Pod 是被 OOMKilled 还是自己退出的。

容器内 `psutil.virtual_memory()` 读到的通常是宿主机视角，不能用于 Pod SLO。

```promql
container_memory_working_set_bytes{namespace="$NS",container="ray-worker"}
/ on(pod)
container_spec_memory_limit_bytes{namespace="$NS",container="ray-worker"}
```

门禁见 [资源怎么设](resource-sizing.md)：P95 ≤ 80% limit，peak ≤ 90%。

## KubeRay：CR 卡在哪一步

KubeRay 1.4.0 起 operator 在 `:8080/metrics` 暴露自定义指标。Helm 要显式打开：

```bash
helm install kuberay-operator kuberay/kuberay-operator \
  --set metrics.serviceMonitor.enabled=true \
  --set metrics.serviceMonitor.additionalLabels.release=prometheus
```

v1.7.0 起 `selector` 改名为 `additionalLabels`。1.6.x 及以前仍用 `selector.release`。装错了 Targets 就是空的。

| 指标 | 回答什么 |
|---|---|
| `kuberay_cluster_info` | 集群存在、属于哪个 owner |
| `kuberay_cluster_condition_provisioned` | 集群供给完成没有 |
| `kuberay_cluster_provisioned_duration_seconds` | **供给花了多久**（RayJob 冷启动成本） |
| `kuberay_job_info` | RayJob 存在 |
| `kuberay_job_deployment_status` | `New` / `Initializing` / `Running` / `Complete` / `Failed` / `Retrying` |
| `kuberay_job_execution_duration_seconds` | 从 Initializing 到终态，带 `retry_count` |
| `controller_runtime_reconcile_total` | operator 自身是否正常调谐 |

CR 卡在 `Initializing`，先看供给耗时和 operator 日志，不要去调 Daft 参数。

## Ray：head 有三个端点

| 端口 | 端口名 | 内容 | 在哪 |
|---:|---|---|---|
| 8080 | `metrics` | 全部 `ray_*` | head **和** 每个 worker |
| 44217 | `as-metrics` | autoscaler | **仅 head** |
| 44227 | `dash-metrics` | dashboard | **仅 head** |

worker 只有一个端点，head 有三个。官方推荐 head 和 worker 用**两个独立的 PodMonitor**。大多数清单只配了 8080，autoscaler 与 dashboard 指标一直缺。

关键 `ray_*`：

| 问题域 | 指标 | 判读 |
|---|---|---|
| 并行度 | `ray_tasks{State}` | `sum(ray_tasks) by (Name,State)`，**必须 sum** |
| actor | `ray_actors{State}` | `RESTARTING` 上升 = 被杀后在重做初始化 |
| 逻辑资源 | `ray_resources{Name="CPU",State}` | USED / AVAILABLE，对照 `ray status` |
| object store | `ray_object_store_memory{Location}` | `SPILLED` / `MMAP_SHM` / `WORKER_HEAP` |
| OOM | `ray_memory_manager_worker_eviction_total` | Ray 自己的 OOM killer 杀了多少 task / actor |
| 节点内存 | `ray_node_cgroup_mem_used` | 容器视角，才能和 cAdvisor 对上 |
| 磁盘 | `ray_node_disk_usage` | spill 与 flight shuffle 打盘 |

`ray_tasks` / `ray_actors` 由多个进程分别上报，含负值点。单独看一条时间序列没有意义，必须 sum。

Ray 2.53 起默认不再导出 `WorkerId` 标签。要恢复：`RAY_metric_cardinality_level=legacy`。

区分两种“杀”：

```text
Ray memory monitor 驱逐    ray_memory_manager_worker_eviction_total
cgroup OOMKilled           kube_pod_container_status_last_terminated_reason
关掉 memory monitor 只是把前者变成后者
```

## Daft：OTLP push

Daft 不开 `/metrics`。设任一 OTLP endpoint 即启用遥测。

| 指标 | 含义 |
|---|---|
| `daft.rows.in` / `daft.rows.out` | 算子消费 / 产出行数 |
| `daft.bytes.read` | source / scan 读入字节（**仅 Ray**） |
| `daft.bytes.written` / `daft.rows.written` | 写出侧 |
| `daft.duration` | 算子累计 CPU 时间（微秒） |
| `daft.task.active` | 当前活跃 task 数 |
| `daft.task.completed` / `.failed` / `.cancelled` | task 终态（**仅 Ray**） |
| `checkpoint.keys_staged` / `.sealed` | 断点续跑；staged 涨、sealed 为 0 = 续跑没生效 |

```bash
# 路线 A：Prometheus 直收
# Prometheus 3.x  --web.enable-otlp-receiver
# Prometheus 2.x  --enable-feature=otlp-write-receiver
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://prometheus.monitoring.svc:9090/api/v1/otlp/v1/metrics

# 路线 B：OTel Collector 中转（生产更常见）
# Daft --OTLP--> Collector --remote_write--> Prometheus
```

| 变量 | 默认 | 注意 |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 未设 | 设了即启用 |
| `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | 未设 | 仅指标，优先级更高 |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | Prometheus 直收必须改 `http/protobuf` |
| `OTEL_METRIC_EXPORT_INTERVAL` | **500 ms** | **默认过于激进**，建议 5000～15000 ms |
| `OTEL_SERVICE_NAME` | `daft` | 不设则 driver 与 worker 分不开 |
| `DAFT_DEV_OTEL_EXPORTER_OTLP_ENDPOINT` | 未设 | **已废弃** |

四个坑：

1. Prometheus 会把 `.` 换成 `_`、给 counter 加 `_total`。落地后用 label values 确认名字，别照着文档写告警。
2. 500ms 间隔 × (driver + 每个 worker) = 可观写入量。节点一多必须调大。
3. `OTEL_SERVICE_NAME` 区分 `daft-ray-driver` / `daft-ray-worker`。
4. `node.id` 是逐算子标签，长期留存要盯 series 数。

内网 / 离线设 `DAFT_ANALYTICS_ENABLED=0`。

## 两个 Dashboard 不互相替代

| 端口 | 谁提供 | 看什么 |
|---:|---|---|
| **8265** | Ray head service | Jobs / Actors / Nodes / Metrics / Logs |
| **3238** | 独立部署的 Daft Dashboard | 查询计划、算子进度、分区级统计 |

Ray Dashboard 随 head Pod 存亡。Daft Dashboard 是被动接收端，按 `DAFT_DASHBOARD_URL` 推送，**可以完全不住在 Ray 集群里**。

Daft Dashboard 限制：状态只在内存、无鉴权、必须和上报的 Daft 同版本。生产部署成独立 Deployment，不要塞进 head 或 driver——RayJob 一回收，你想留的那段历史正好没了。它不可达时 Daft 只打 warning，作业照常跑，所以它不能当门禁。

## PodMonitor 硬契约

```text
1  head 和 worker 的 rayStartParams 都设 metrics-export-port: "8080"
2  containerPort 必须命名为 metrics（以及 head 的 as-metrics / dash-metrics）
3  head 与 worker 用两个独立的 PodMonitor
   head 选 ray.io/node-type=head    抓 8080 + 44217 + 44227
   worker 选 ray.io/node-type=worker 只抓 8080
4  不要用 ServiceMonitor 抓 head（RayService 会建两个 Service，指标抓两遍）
5  relabel 同时保留 ray_cluster 与 ray_io_cluster
6  KubeRay operator 单独一个 ServiceMonitor
7  Targets 为空先查 namespace selector、PodMonitor selector、Helm release label
```

```yaml
# 示意：worker 只抓 metrics
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: ray-workers
spec:
  selector:
    matchLabels:
      ray.io/node-type: worker
  podMetricsEndpoints:
    - port: metrics
      interval: 15s
```

Grafana：Ray 官方看板 JSON 在 head Pod 里。

```bash
kubectl -n "$NS" cp "$HEAD":/tmp/ray/session_latest/metrics/grafana/dashboards/ /tmp/
```

要在 Ray Dashboard 内嵌 Grafana，head 上三个变量：`RAY_GRAFANA_HOST`（后端健康检查）、`RAY_GRAFANA_IFRAME_HOST`（浏览器取图）、`RAY_PROMETHEUS_HOST`。前两个不是一回事，配混了页面就是空白。
