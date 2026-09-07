# 如何看日志

“日志找不到”的根因：`kubectl logs` 只覆盖第 1 类。Ray 真正的现场在 Pod 内的 `/tmp/ray`。

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

不要用 hostPath 当“持久化”：节点宕机正是最需要日志的时候，它跟着节点一起没。没有 Loki / ES 时，Fluent Bit 也可以直接写 S3——流式 tail，崩溃前最后一段还能留下。周期 `aws s3 sync` 不行：`OOMKilled` 是 SIGKILL，不走 preStop。

## 两个让日志变干净的开关

```text
RAY_DEDUP_LOGS=0
  关掉去重，否则报错被折叠成 [repeated 99x]

LoggingConfig(encoding="JSON") + log_to_driver=False
  结构化输出，带 job_id / task_id / actor_id
```

长作业注意轮转：默认 512MB × 5 份。刷屏时最早的启动日志会被转掉。`RAY_ROTATION_MAX_BYTES` / `RAY_ROTATION_BACKUP_COUNT` 按预期日志量算够。
