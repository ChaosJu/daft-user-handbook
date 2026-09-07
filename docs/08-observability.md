# 日志与监控

观测分四层，和部署栈一一对应。**没有任何一层能替代另一层。** 排障顺序永远是自下而上：K8s → KubeRay → Ray → Daft。下层没起来，上层指标不会产生。

| 层 | 回答什么 | 指标出口 | 日志出口 |
|---|---|---|---|
| **K8s** | Pod 活着吗、被 OOMKilled 了吗、CPU 被限流了吗 | cAdvisor + kube-state-metrics | 容器 stdout → kubelet |
| **KubeRay** | CR 进到哪一步、集群供给花了多久 | operator `:8080` | operator Deployment 日志 |
| **Ray** | task / actor 状态、object store、spill、控制面延迟 | head 三个端点 + 每个 worker `:8080` | `/tmp/ray/session_latest/logs/` |
| **Daft** | 算子行数 / 字节、task 终态、查询计划 | **OTLP push，不是 `/metrics` pull** | driver / worker stdout + `events.jsonl` |

```text
节点 kubelet /metrics/cadvisor ─┐
kube-state-metrics              ─┤
kuberay-operator :8080          ─┼──►  Prometheus  ──►  Grafana
ray head :8080 / :44217 / :44227─┤
ray worker :8080  × N           ─┘

driver / worker ──OTLP──► Collector / Prometheus OTLP receiver ──► Prometheus
```

前三层用 PodMonitor / ServiceMonitor 去抓。Daft **不开 `/metrics` 端口**，PodMonitor 抓不到它，必须设 `OTEL_EXPORTER_OTLP_*` 让它往外推。

## 五个常见缺口

默认部署一路装下来，基本都会缺这五样。按"离生产可用还差多远"排序：

| # | 缺口 | 后果 | 补法 |
|---:|---|---|---|
| 1 | **Ray 文件日志无持久化** | Pod 一没，失败现场全丢 | Fluent Bit sidecar 采 `/tmp/ray` |
| 2 | **Daft OTLP 未接通** | 算子行数、task 失败率进不了 Prometheus | 开 OTLP receiver 或上 Collector |
| 3 | head 只抓了 `:8080` | autoscaler 与 dashboard 指标缺失 | head 单独 PodMonitor，补 44217 / 44227 |
| 4 | KubeRay operator 指标未接 | 看不到供给耗时与 CR 状态 | helm 开 `metrics.serviceMonitor.enabled` |
| 5 | 产物与 Dashboard 均为易失态 | 报告与查询计划随重启消失 | 产物写对象存储，别留在 Pod 本地 |

第 1 条是"出了事查不了"，优先级最高。第 2 条是"出了事看不见"。3～5 是"看得不全"。

---

# 一 · 日志

"日志找不到"的根因：`kubectl logs` 只覆盖第 1 类。Ray 真正的现场在 Pod 内的 `/tmp/ray`。

## 五个来源

| # | 来源 | 落在哪 | `kubectl logs` |
|---:|---|---|---|
| 1 | 容器 stdout / stderr | kubelet → `/var/log/pods` | 能 |
| 2 | Ray 系统与组件日志 | `/tmp/ray/session_latest/logs/` | **不能** |
| 3 | Ray driver 日志 | 同上，`job-driver-<submission_id>.log` | **不能** |
| 4 | UDF / worker 进程输出 | 同上，`worker-*.out` / `python-core-worker-*.log` | **不能** |
| 5 | submitter Pod 日志 | submit Job 的 Pod stdout | 能，但只有转发的那部分 |

```text
/tmp/ray 默认是容器可写层或 emptyDir
→ Pod 一重建，第 2～4 条全部消失
→ 生产不做持久化，就等于「失败之后没有日志」
```

## Ray 日志文件对照

```bash
kubectl -n "$NS" exec -c ray-head "$HEAD" -- ls -lt /tmp/ray/session_latest/logs
```

| 文件 | 内容 | 什么时候看 |
|---|---|---|
| `job-driver-<submission_id>.log` | Jobs API 提交的 driver stdout | driver 失败，第一个看这个 |
| `worker-<wid>-<jid>-<pid>.[out\|err]` | task / actor 的 Python stdout / stderr | UDF 抛错、模型加载失败 |
| `python-core-worker-<wid>_<pid>.log` | worker 的 C++ core 日志 | 进程被杀、段错误 |
| `python-core-driver-<wid>_<pid>.log` | driver 的 C++ core 日志 | driver 侧连接 / 调度异常 |
| `raylet.[out\|err]` | raylet | 节点掉线、资源上报异常 |
| `gcs_server.[out\|err]` | GCS（**仅 head**） | 控制面异常、元数据压力 |
| `monitor.[log\|out\|err]` | autoscaler | 扩缩容不动 |
| `dashboard.[log\|out\|err]` | Ray Dashboard | 8265 打不开 |
| `runtime_env_setup-<job_id>.log` | runtime_env 安装过程 | 依赖冲突、pip 装包失败 |
| `log_monitor.[log\|out\|err]` | worker → driver 日志转发 | driver 上看不到 worker 输出 |
| `io-worker-*.[out\|err]` | spill / restore IO worker | object spill 异常 |

```text
.out = stdout    .err = stderr    .log = 该组件自己 logger 写的
```

## 五种查看方式

从最快到最全：

```text
① 最快   kubectl logs                     只有 stdout，够看崩溃栈
② 常用   ray job logs <submission_id>     driver 全量输出，日常首选
③ 直观   Dashboard 8265 → Logs 页签       按节点 / 进程浏览，仅当前会话有效
④ 最全   exec 进 Pod 翻 /tmp/ray/.../logs 唯一能看到 worker 内部的方式
⑤ 生产   集中式日志（Loki / ES）          唯一能在 Pod 消失之后还查得到的方式
```

```bash
# ① 容器 stdout（submitter 或 head）
kubectl -n "$NS" logs "$POD" -c ray-head --tail=200
kubectl -n "$NS" logs job/daft-nightly-xxxxx --tail=200

# ② driver 日志（最常用）
kubectl -n "$NS" exec -c ray-head "$HEAD" -- \
  ray job logs --address http://127.0.0.1:8265 "$JOB_ID" | tail -200

# ③ Dashboard
kubectl -n "$NS" port-forward svc/daft-head-svc 8265:8265
# 浏览器打开 http://127.0.0.1:8265 → Logs

# ④ 在 worker 里搜异常，UDF 内部报错通常只在这里
kubectl -n "$NS" exec -c ray-worker "$W" -- \
  sh -c 'grep -rlE "Traceback|Error|Killed" /tmp/ray/session_latest/logs | head'

# 列出最近日志
kubectl -n "$NS" exec -c ray-head "$HEAD" -- \
  ls -lt /tmp/ray/session_latest/logs | head
```

①～④ 有一个共同前提：**Pod 还在**。RayJob 的 TTL 一到或 `shutdownAfterJobFinishes` 回收集群，只剩 ⑤。

## 失败之后的固定顺序

先别删任何东西。`kubectl delete` 之后 Pod、日志、events 一起消失。

```bash
# 锁定失败层次
kubectl -n "$NS" get pods,jobs,rayjobs,rayclusters

# K8s 判定
kubectl -n "$NS" describe pod "$POD"
# lastState.terminated.reason == OOMKilled  → 内存问题

# KubeRay 判定
kubectl -n "$NS" get rayjob "$NAME" \
  -o jsonpath='{.status.jobDeploymentStatus}{"\n"}{.status.jobStatus}{"\n"}'
kubectl -n kuberay-system logs deploy/kuberay-operator | grep "$CLUSTER"

# Ray 判定
kubectl -n "$NS" exec -c ray-head "$HEAD" -- ray job status "$JOB_ID"
kubectl -n "$NS" exec -c ray-head "$HEAD" -- \
  ray list actors --address http://127.0.0.1:8265
```

三种失败形态，日志去处不同：

| 形态 | 典型证据 | 日志去哪找 |
|---|---|---|
| Pod 起不来 | Pending / ImagePullBackOff | `describe pod` 的 Events |
| driver 失败 | `ray job status` = FAILED，退出码 1 | `job-driver-<sid>.log` |
| worker / actor 被杀 | 退出码 137，actor `RESTARTING` | `worker-*.err`、`python-core-worker-*.log` |

```text
退出码
0    正常
1    Python 异常
2    参数错误
137  SIGKILL（OOM）—— 可能是 cgroup，也可能是 Ray memory monitor
139  段错误（原生库与镜像不匹配）
143  SIGTERM
```

取诊断包时不要只拷 head。被杀的那个进程日志通常在 **worker** 的 `/tmp/ray` 里。Pod 本地产物是 emptyDir，一并带走。

RayJob 的 `.status.reason` 能直接告诉你卡在哪一层，对照表见 [KubeRay RayJob 部署](03-deploy-kuberay.md)。

## 生产必须做持久化

| 方案 | 覆盖 | 结论 |
|---|---|---|
| DaemonSet 采 stdout | 只有第 1 类 | 必要但**不充分** |
| **Fluent Bit sidecar 采 `/tmp/ray`** | 第 2～4 类 | **官方推荐，生产必须做** |
| `RAY_LOG_TO_STDERR=1` | 全部转 stderr | **官方不推荐**（转发和 Dashboard 失效） |

sidecar 要点：

1. Ray 容器和 sidecar 共享 emptyDir，双方都 mount 到 `/tmp/ray`。
2. Fluent Bit `Refresh_Interval` 设 5（默认 60），因为 `session_latest/logs/` 是 Ray 起来之后才创建的。
3. 用 downward API 把 `ray.io/cluster` 注入成日志 label。

不要用 hostPath 当"持久化"：节点宕机正是最需要日志的时候，它跟着节点一起没。没有 Loki / ES 时，Fluent Bit 也可以直接写 S3——流式 tail，崩溃前最后一段还能留下。周期 `aws s3 sync` 不行：`OOMKilled` 是 SIGKILL，不走 preStop。

## 两个让日志变干净的开关

```text
RAY_DEDUP_LOGS=0
  关掉去重，否则报错被折叠成 [repeated 99x]

LoggingConfig(encoding="JSON") + log_to_driver=False
  结构化输出，带 job_id / task_id / actor_id
```

长作业注意轮转：默认 512MB × 5 份。刷屏时最早的启动日志会被转掉。`RAY_ROTATION_MAX_BYTES` / `RAY_ROTATION_BACKUP_COUNT` 按预期日志量算够。

---

# 二 · 指标

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

cAdvisor 回答"用了多少"，kube-state-metrics 回答"发生了什么"。只有后者能告诉你 Pod 是被 OOMKilled 还是自己退出的。

容器内 `psutil.virtual_memory()` 读到的通常是宿主机视角，不能用于 Pod SLO。

```promql
container_memory_working_set_bytes{namespace="$NS",container="ray-worker"}
/ on(pod)
container_spec_memory_limit_bytes{namespace="$NS",container="ray-worker"}
```

门禁阈值见[资源与调参](09-tuning-runbook.md)。

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

区分两种"杀"：

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

指标看出问题之后动哪个旋钮，见[资源与调参](09-tuning-runbook.md)。
