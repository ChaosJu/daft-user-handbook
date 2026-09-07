# 观测总览

观测分四层，和部署栈一一对应。**没有任何一层能替代另一层。** 排障顺序永远是自下而上：K8s → KubeRay → Ray → Daft。下层没起来，上层指标不会产生。

## 四层

| 层 | 回答什么 | 指标出口 | 日志出口 |
|---|---|---|---|
| **K8s** | Pod 活着吗、被 OOMKilled 了吗、CPU 被限流了吗 | cAdvisor + kube-state-metrics | 容器 stdout → kubelet |
| **KubeRay** | CR 进到哪一步、集群供给花了多久 | operator `:8080` | operator Deployment 日志 |
| **Ray** | task / actor 状态、object store、spill、控制面延迟 | head 三个端点 + 每个 worker `:8080` | `/tmp/ray/session_latest/logs/` |
| **Daft** | 算子行数 / 字节、task 终态、查询计划 | **OTLP push，不是 `/metrics` pull** | driver / worker stdout + `events.jsonl` |

## 指标：前三层 pull，Daft 是 push

```text
节点 kubelet /metrics/cadvisor ─┐
kube-state-metrics              ─┤
kuberay-operator :8080          ─┼──►  Prometheus  ──►  Grafana
ray head :8080 / :44217 / :44227─┤
ray worker :8080  × N           ─┘

driver / worker ──OTLP──► Collector / Prometheus OTLP receiver ──► Prometheus
```

前三层用 PodMonitor / ServiceMonitor 去抓。Daft **不开 `/metrics` 端口**，PodMonitor 抓不到它。必须设 `OTEL_EXPORTER_OTLP_*`，让 Daft 往外推。两条链路最终可以进同一个 Prometheus，但配置方式完全不同。

## 日志：`kubectl logs` 只看见 stdout

```text
容器 stdout/stderr ──► kubelet ──► /var/log/pods     ← kubectl logs 只到这里
/tmp/ray/session_latest/logs/*                       ← 必须 sidecar / 集中式采集
```

`/tmp/ray` 默认是 emptyDir 或容器可写层。Pod 一重建，Ray driver、UDF、raylet、GCS 的现场全部消失。这是“失败之后没有日志”的根因。

## 五个常见缺口

默认部署一路装下来，基本都会缺这五样。按“离生产可用还差多远”排序：

| # | 缺口 | 后果 | 补法 |
|---:|---|---|---|
| 1 | **Ray 文件日志无持久化** | Pod 一没，失败现场全丢 | Fluent Bit sidecar 采 `/tmp/ray` |
| 2 | **Daft OTLP 未接通** | 算子行数、task 失败率进不了 Prometheus | 开 OTLP receiver 或上 Collector |
| 3 | head 只抓了 `:8080` | autoscaler 与 dashboard 指标缺失 | head 单独 PodMonitor，补 44217 / 44227 |
| 4 | KubeRay operator 指标未接 | 看不到供给耗时与 CR 状态 | helm 开 `metrics.serviceMonitor.enabled` |
| 5 | 产物与 Dashboard 均为易失态 | 报告与查询计划随重启消失 | 产物写对象存储，别留在 Pod 本地 |

第 1 条是“出了事查不了”，优先级最高。第 2 条是“出了事看不见”。3～5 是“看得不全”。

## 本章怎么读

| 页 | 用途 |
|---|---|
| [如何看日志](logs.md) | 五个来源、Ray 文件对照、五种查看方式 |
| [如何看监控](monitoring.md) | K8s / KubeRay / Ray / Daft 指标与两个 Dashboard |
| [根据监控调参](tune-from-metrics.md) | 症状 → 先看什么 → 动哪个旋钮 |
| [资源怎么设](resource-sizing.md) | worker 少而大、内存公式、起步配方 |

还在跑的时候先做三十秒体检（见 [根据监控调参](tune-from-metrics.md)）。失败之后先别删任何东西，再按 K8s → KubeRay → Ray → Daft 往下拆。
