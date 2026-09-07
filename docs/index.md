# Daft 用户指导手册

面向要在 **Ray / KubeRay** 上把 Daft 跑进生产的工程师。官方 API 以 [docs.daft.ai](https://docs.daft.ai) 为准，本手册写的是生产共识，不是签名百科。

九页，按阅读顺序编号。每个事实只在一页里展开，其余地方只留链接。

| 页 | 回答什么 |
|---|---|
| [1. 架构](01-architecture.md) | Flotilla / Swordfish、pipeline 而非 stage、算子分两类、三条部署硬约束、四个调参维度 |
| [2. 执行模型](02-execution-model.md) | 惰性求值与触发执行、Runner、partition / morsel / batch、morsel 与 into_batches |
| [3. KubeRay RayJob 部署](03-deploy-kuberay.md) | RayJob 原理、Quickstart 跑通、关键 YAML 字段 |
| [4. Partition](04-partition.md) | task 数、scan 切分、shuffle 边界、into_batches 在 Ray 上的分区效应 |
| [5. UDF](05-udf.md) | `@daft.func` / `.batch` / `@daft.cls`、`max_concurrency` 两种语义、actor 两个不等式 |
| [6. 读写参数](06-io-config.md) | IOConfig、Parquet / Lance 读写、`download` 并发、`write_mode` |
| [7. 日志与监控](07-observability.md) | 失败怎么查日志、关键指标与看板截图 |
| [8. 资源与调参](08-tuning-runbook.md) | worker 规格、内存预算、morsel 调参、症状 → 动作 |
| [9. 生产禁区](09-production-donts.md) | 上线前逐条勾的门禁清单 |

可 apply 的清单在 [`examples/`](../examples/)：手上没镜像先跑 [`quickstart/`](../examples/quickstart/)，生产参考 [`kuberay/`](../examples/kuberay/)，自建镜像模板在 [`docker/`](../examples/docker/)。

## 四条要先记住的规则

### 1. DataFrame 是惰性的，生产以 `write_*` 收尾

`select`、`where`、`with_column`、`join` 只构造 LogicalPlan。把大结果 `collect()` / `to_pandas()` 拉回 driver，等于把分布式作业退化成单机内存拷贝。完整的触发执行清单见[执行模型](02-execution-model.md)。

### 2. 并行度看 partition，单 task 峰值内存看 morsel

`into_partitions(N)` 解决"几个 task 一起跑"，`into_batches(n)` 解决"每个 task 一次推多少行"。两者不能互相替代，也不能只调一个就指望峰值内存下来。并行度见 [Partition](04-partition.md)，morsel 机制见[执行模型](02-execution-model.md)。

### 3. 调参从上往下，每轮只改一个变量

资源到位 → partition → morsel → UDF → I/O → 写出布局。上层决定下层的输入形状：partition 还没定就调 morsel，等于在会变的分母上找拐点。完整顺序和纪律见[资源与调参](08-tuning-runbook.md)。

### 4. 排障自下而上：K8s → KubeRay → Ray → Daft

下层没起来，上层指标不会产生。`kubectl logs` 只覆盖容器 stdout，Ray 真正的现场在 Pod 里的 `/tmp/ray`，Pod 一重建就没了。见[日志与监控](07-observability.md)。
