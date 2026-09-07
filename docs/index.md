# Daft 用户指导手册

面向要在 **Ray / KubeRay** 上把 Daft 跑进生产的工程师。官方 API 以 [docs.daft.ai](https://docs.daft.ai) 为准，本手册写的是生产共识，不是签名百科。

十页，按阅读顺序编号。每个事实只在一页里展开，其余地方只留链接。

| 页 | 回答什么 |
|---|---|
| [1. 架构](01-architecture.md) | Flotilla / Swordfish、pipeline 而非 stage、算子分两类、三条部署硬约束、四个旋钮 |
| [2. 执行模型](02-execution-model.md) | 惰性求值与触发执行、Runner、partition / morsel / batch 三层、内存由什么构成 |
| [3. KubeRay RayJob 部署](03-deploy-kuberay.md) | 装 operator、apply 清单、字段对照、失败判层 |
| [4. Partition](04-partition.md) | task 数、scan 切分、shuffle 边界、计算分区 ≠ 写出分区 |
| [5. Morsel 与 into_batches](05-morsel-batch.md) | 行批要求怎么传播、两个旋钮的真实差别、什么时候压批 |
| [6. UDF](06-udf.md) | `@daft.func` / `.batch` / `@daft.cls`、`max_concurrency` 两种语义、actor 两个不等式 |
| [7. 读写参数](07-io-config.md) | IOConfig、Parquet / Lance 读写、`download` 并发、`write_mode` |
| [8. 日志与监控](08-observability.md) | 四层观测、Ray 日志到底在哪、指标怎么接出来 |
| [9. 资源与调参](09-tuning-runbook.md) | worker 规格、内存预算、症状 → 动作、调参纪律 |
| [10. 生产禁区](10-production-donts.md) | 上线前逐条勾的门禁清单 |

可 apply 的清单在 [`examples/`](../examples/)：手上没镜像先跑 [`quickstart/`](../examples/quickstart/)，生产参考 [`kuberay/`](../examples/kuberay/)，自建镜像模板在 [`docker/`](../examples/docker/)。

## 四条要先记住的规则

### 1. DataFrame 是惰性的，生产以 `write_*` 收尾

`select`、`where`、`with_column`、`join` 只构造 LogicalPlan。把大结果 `collect()` / `to_pandas()` 拉回 driver，等于把分布式作业退化成单机内存拷贝。完整的触发执行清单见[执行模型](02-execution-model.md)。

### 2. 并行度看 partition，单 task 峰值内存看 morsel

`into_partitions(N)` 解决"几个 task 一起跑"，`into_batches(n)` 解决"每个 task 一次推多少行"。两者不能互相替代，也不能只调一个就指望峰值内存下来。见 [Partition](04-partition.md) 与 [Morsel](05-morsel-batch.md)。

### 3. 调参从上往下，每轮只改一个变量

资源到位 → partition → morsel → UDF → I/O → 写出布局。上层决定下层的输入形状：partition 还没定就调 morsel，等于在会变的分母上找拐点。完整顺序和纪律见[资源与调参](09-tuning-runbook.md)。

### 4. 排障自下而上：K8s → KubeRay → Ray → Daft

下层没起来，上层指标不会产生。`kubectl logs` 只覆盖容器 stdout，Ray 真正的现场在 Pod 里的 `/tmp/ray`，Pod 一重建就没了。见[日志与监控](08-observability.md)。
