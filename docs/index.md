# Daft 用户指导手册

本手册面向要在 **Ray / KubeRay** 上把 Daft 跑进生产的工程师。它按四章组织，对应四件必须先想清楚的事：系统长什么样、数据怎么流动、生产怎么配、出事怎么看。

## 四章一览

| 章 | 内容 | 先读哪一页 |
|---|---|---|
| [1. 架构介绍](01-architecture/index.md) | Flotilla / Swordfish、三层计划、流式执行、部署硬约束、四个旋钮 | 整章一页读完 |
| [2. 基本原理](02-principles/index.md) | Lazy DataFrame、Runner、Partition 与 Morsel、多内存池 | 再读 [Morsel](02-principles/morsel.md) |
| [3. 最佳实践](03-best-practices/index.md) | KubeRay、[RayJob 实战](03-best-practices/rayjob-hands-on.md)、分区、批、UDF、I/O、[生产禁区](03-best-practices/production-donts.md) | 要 apply 先读 [RayJob 实战](03-best-practices/rayjob-hands-on.md) |
| [4. 日志、监控与调参](04-observability/index.md) | 四层观测、日志、指标、按监控调参、资源配方 | 出事先读 [怎么看日志](04-observability/logs.md) |

官方 API 以 [docs.daft.ai](https://docs.daft.ai) 为准。本手册写的是生产共识，不是签名百科。

## 四条要记住的规则

### 1. DataFrame 是惰性的。生产以 `write_*` 收尾，不是 `collect`

`select`、`where`、`with_column`、`join` 只构造 LogicalPlan。真正执行发生在 `show`、`collect`、`count_rows`、`to_pydict`、`to_arrow` 以及所有 `write_*`。

生产作业的终点必须是对象存储或表格式上的 sink。把大结果 `collect()` / `to_pandas()` 拉回 driver，等于把分布式作业退化成单机内存拷贝。

### 2. 分布式并行度看 partition；单 task 峰值内存看 morsel / batch

- **Partition** 是 Flotilla 切出来的 task，决定并行度、重算粒度和写出文件数。
- **Morsel / batch** 是 Swordfish 在单个 task 内部流动的行批，决定在途内存和 UDF 单批峰值。

`into_partitions(N)` 解决“几个 task 一起跑”。`into_batches(n)` 解决“每个 task 一次拿多少行”。两者不能互相替代。

### 3. 调参顺序：资源到位 → partition → morsel → UDF → I/O → write。每轮只改一个变量

```text
确认 worker / CPU / 内存真的到位
        ↓
sweep partition（1× / 2× / 4× 总 CPU）
        ↓
sweep morsel 或局部 into_batches
        ↓
sweep UDF actor / batch_size / max_concurrency
        ↓
sweep 对象存储与 download 并发
        ↓
最后才改写出布局
```

上层决定下层的输入形状。partition 没定就调 morsel，等于在会变的分母上找拐点。代码、镜像、数据快照、worker 规格保持不变。

### 4. 观测自下而上：K8s → KubeRay → Ray → Daft

下层没起来，上层指标不会产生。`kubectl logs` 只看到容器 stdout；Ray 真正的现场在 `/tmp/ray`。排障顺序永远是：Pod 活着吗 → CR 卡在哪 → task/actor 什么状态 → Daft 算子停在哪。

---

读完这四条再进各章。要直接在集群上跑，从 [RayJob 实战](03-best-practices/rayjob-hands-on.md) 开始：手上没镜像先用 [`examples/quickstart/`](../examples/quickstart/)（官方镜像 + 官方示例，两个文件零构建），生产参考看 [`examples/kuberay/`](../examples/kuberay/)。上线前过一遍 [生产禁区](03-best-practices/production-donts.md)。
