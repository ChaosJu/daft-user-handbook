# Morsel 驱动的流式执行

Swordfish 不以“整张表”或“整个 partition”为单位执行。数据被切成 **morsel（行批）**，由 source 向上推送，在有界 async channel 里流动。下游处理不过来就背压，上游停下。这是峰值内存能和数据总量脱钩的原因。

## 数据怎么推

```text
source 产出 morsel
  → 有界 channel
  → 下游算子消费
  → channel 满则上游阻塞（背压）
```

`default_morsel_size` 是单个 morsel 的**行数上限**，不是保证值。实际行数随数据波动。真正按运行时反馈调批，是另一个开关 `enable_dynamic_batching`，**默认关闭**。

峰值内存 ≈ 并发 task × 在途 morsel 数 × 单行实际体积。数据总量变大，只要在途 morsel 不变，峰值可以不变。

**例外：blocking sink。** `aggregate`、`sort`、`join` build 侧、`repartition` 等必须收齐输入，状态会随数据增长。这时调小 morsel 几乎无效。先看 `explain()` 里有没有这类算子。

## 默认值与唯一生效方式

```text
default_morsel_size = 131072 行
单位是行，不是字节
```

这个默认值是为窄表标量列设的。URL、blob、解码图像、embedding 上继续用 131072，单批就能到数 GB。

**Daft 不读 `DAFT_DEFAULT_MORSEL_SIZE` 环境变量。** 这个名字看起来合理，但 `DaftExecutionConfig::from_env` 的白名单里没有它——白名单只有 `DAFT_SHUFFLE_ALGORITHM`、`DAFT_SCANTASK_MAX_PARALLEL`、`DAFT_NATIVE_PARQUET_WRITER`、`DAFT_MIN_CPU_PER_TASK`、`DAFT_ACTOR_UDF_READY_TIMEOUT`、`DAFT_MAINTAIN_ORDER` 和四个 inflation factor。只设环境变量不报错、不告警，morsel 静默保持 131072。

真正生效的入口只有一个：

```python
import daft

daft.set_execution_config(default_morsel_size=64)
```

那为什么很多部署里设了这个环境变量却确实有效？因为**应用自己把它读出来再转进去**了。这是完全正当的做法，也是本手册示例清单的用法——`00-platform.yaml` 的 `bench-env` 里设 `DAFT_DEFAULT_MORSEL_SIZE: "8"`，应用侧长这样：

```python
# config.py
DAFT_DEFAULT_MORSEL_SIZE = env_int("DAFT_DEFAULT_MORSEL_SIZE", 32)

# 流水线入口
daft.set_execution_config(default_morsel_size=config.DAFT_DEFAULT_MORSEL_SIZE)
```

区别在于责任方：读环境变量的是你的代码，不是 Daft。所以判断一个部署里这个变量有没有用，不能看 YAML，要去看入口有没有那一行 `set_execution_config`。

推论：`set_execution_config` 里的字段，都不要默认有同名环境变量。想用环境变量驱动，就自己在入口显式转一遍，并且新增参数时照同一条路走——否则就是死配置。

## `into_batches` 是局部覆盖

全局 morsel 不动，只在膨胀算子前把批压小，通常比全局调小 `default_morsel_size` 更省吞吐。

```python
df = (
    df.into_batches(16)
    .with_column("image", daft.col("bytes").image.decode())
    .with_column("emb", embed_fn(daft.col("image")))
)
```

规则：

- `batch_size` 必须大于 0，单位是行，不是字节。
- 当前按 best effort 执行，达到约 `batch_size * 0.8` 时可能发出批次。
- 最后一批通常是余数。
- Native 和 Ray 都有效。这是单机调试内存时最有用的旋钮。

适用时机：download、decode、explode、模型推理等**单行会膨胀**的操作之前。scan 阶段行还很瘦时，不必提前压批。

`@daft.func.batch` 的 `batch_size` 是单批上限，不是保证值。上游 morsel 更小就填不满。它与 `enable_dynamic_batching` 同时打开会冲突。

## 动态批

```python
daft.context.set_execution_config(
    enable_dynamic_batching=True,
    dynamic_batching_strategy="auto",  # 或 latency_constrained
)
```

默认关。打开后引擎按运行时反馈调批。生产调参时先把静态 morsel 跑稳，再评估动态批，不要两件事一起开。

## 按行形态选起点

| 行形态 | 起点（行） | 典型列 |
|---|---:|---|
| 窄表、标量列 | 1024 ～ 131072 | int / float / 短字符串 |
| 大字符串、百 KB 级对象 | 16 ～ 64 | 长文本、小图片 bytes、JSON blob |
| MB 级对象、解码结果 | 4 ～ 16 | 解码图像、解压后文档 |
| 大 tensor / 重中间状态 | 1 ～ 8 | 视觉模型输入、大 embedding |

起点不是最优值。用 1× / 2× / 4× 做小范围 sweep，同时看 working set 与吞吐。

## 何时减小、何时增大

**减小 morsel / `into_batches`：**

- OOM，或 working set 逼近 Pod limit
- Object Store spill 上升
- decode / explode / UDF 之后单行体积暴涨
- actor 因内存反复 `RESTARTING`

**增大 morsel：**

- CPU 低，但内存安全（P95 working set < 60% limit）
- 调度开销高、极碎的小批占满时间线
- 窄表标量链路，模型吞吐吃不满

**调小无效时：** pipeline 里有 blocking sink。先改算法、减 key 基数、换 broadcast / 去 shuffle，再回头看 morsel。

morsel 过小会推高调度开销与尾部 task 数。不要从 131072 直接砍到 1，除非行已经是 MB 级。

## Streaming 与 blocking 清单

**Streaming**（来一批走一批）：

`project` / `filter` / `explode` / `unpivot` / `into_batches` / UDF / `limit` / `sample` / `monotonically_increasing_id`

**Blocking**（收齐才产出）：

- 聚合：`aggregate`、`grouped_aggregate`、`pivot`、`distinct`
- 排序：`sort`、`top_n`
- 窗口：`window`
- 重分区：`repartition`、`into_partitions`
- Join：build 侧
- 写出：`write`、`commit_write`

WriteSink 虽然是 blocking，但按 morsel 滚动落盘，不把全表攒在内存。真正放大写出内存的是 row group 缓冲和高基数 `partition_cols`。

## `maintain_order`

默认 `True`。保序会引入排序缓冲，部分算子更重。

不需要输出顺序时：

```python
daft.context.set_execution_config(maintain_order=False)
```

也可用环境变量 `DAFT_MAINTAIN_ORDER=false`（这个名字在白名单里）。

`write_parquet` 等 sink 可能强制关掉保序并向下传播。下游如果依赖行序，不要假设写出之后还保持输入顺序。

## 和另外三个旋钮的关系

```text
partition     决定有多少个 task 同时跑
morsel        决定每个 task 一次推多少行
batch_size    决定 UDF 一次推理多少行（≤ 上游 morsel）
max_concurrency  决定有多少个模型实例 / 协程同时吃这些批
```

四个数字相乘才是在途字节。只改其中一个、不看乘积，就会出现“我已经把 morsel 调小了还是 OOM”——因为 actor 数或 `download(max_connections=32)` 把并发乘回去了。
