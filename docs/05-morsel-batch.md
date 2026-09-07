# Morsel 与 into_batches

这一页管**单 task 内部**的峰值内存。分布式并行度看 [Partition](04-partition.md)，算子为什么分两类看[架构](01-architecture.md)。

## 两个旋钮不是一回事

常见误解是"`default_morsel_size` 和 `into_batches` 作用一样，只是一个全局一个局部"。它们确实共用同一套 `MorselSizeRequirement` 机制、单位都是**行**，但语义差三点，配错方向就不生效。

| | `default_morsel_size` | `into_batches(n)` |
|---|---|---|
| 是什么 | execution config 里的一个数 | **计划里的一个节点**，出现在 `explain()` 里 |
| 注入的要求 | `Flexible(0, N)` | `Flexible(⌊0.8n⌋, n)` |
| 下界 | **0——从不攒批**，缓冲区来多少发多少 | **0.8n——攒够才发**，这才是"best effort"的含义 |
| 作用范围 | 整条 pipeline 的兜底值 | 从该节点**向上游**传播，直到最近的 blocking sink |
| 在 Ray 上 | 只影响 task 内部 | **是一个 task 边界，会重切 partition** |

所以：想让 scan 少读几行，`into_batches` 有效；想给整条链路定一个保守默认值，才用 `default_morsel_size`。两者都调时以交集为准。

## 行批要求怎么传播

理解这一节，才能判断 `into_batches` 该插在哪。

物理计划树的根是 sink，子节点是上游。引擎从根开始，把要求**沿树往子节点递归**——也就是沿数据流**往上游**推：

```text
起点        根节点收到 Flexible(0, default_morsel_size)
每个算子    effective = combine(自己的要求, 从下游收到的要求)
            然后把 effective 继续传给上游
source      直接把收到的要求当作 chunk_size 读数据
blocking    忽略下游要求，给上游重新发 default —— 要求穿不过 blocking sink
```

`combine` 的规则：`Strict` 一律优先；两个 `Flexible` 取区间交集（下界取 max，上界取 min）；区间不重叠时回退到下游那个。

每个算子有自己的缓冲区，按 effective 区间决定什么时候发批：

```text
行数 < 下界     不发，继续攒
行数在区间内     全部发出
行数 > 上界     切一片上界大小发出，余数留在缓冲区
```

下界为 0 的算子永远落在"区间内"，所以它**从不攒批**——上游给多少就往下游推多少。这就是为什么 `into_batches(16)` 之后的 project / UDF 不会把 16 行的 morsel 重新攒回 131072 行：它们的下界是 0。

两个推论直接决定用法：

- **`into_batches` 要插在膨胀算子之前**。它约束的是自己和上游（包括 scan 的读取粒度）。插在 decode 之后，对 decode 本身毫无作用。
- **要求穿不过 blocking sink**。`sort` / `aggregate` / `join` build 侧下游的 `into_batches`，管不到它上游的 scan。

背靠背的两个 `into_batches` 会被优化器（`DropIntoBatches`）折叠，**保留下游那个**：`.into_batches(10).into_batches(5)` 等价于 `.into_batches(5)`。

## `default_morsel_size`：默认值与唯一生效方式

```text
default_morsel_size = 131072 行（128 × 1024）
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

## `into_batches`：在膨胀点之前压批

全局 morsel 不动，只在膨胀算子前把批压小，通常比全局调小 `default_morsel_size` 更省吞吐。

典型膨胀点：`download()` / 读对象存储字节、图像 decode 与 resize、解压、JSON 展开、`explode`、模型推理与 embedding。

```python
import daft

daft.context.set_execution_config(default_morsel_size=8192)  # 窄列阶段保持吞吐

df = (
    daft.read_parquet("s3://bucket/meta/*.parquet")
    .select("id", "url")
    .into_batches(16)  # 下载 / 解码前压到 16 行
    .with_column("bytes", daft.col("url").url.download(max_connections=8))
    .with_column("image", daft.col("bytes").image.decode())
    .with_column("emb", embed_fn(daft.col("image")))
)
df.write_lance("s3://bucket/out.lance", mode="overwrite")
```

scan 阶段行还很瘦（几十字节的 URL），用默认甚至较大的 morsel 没问题。危险从"瘦列变胖列"那一步开始。

规则：

- `batch_size` 必须 `> 0`，单位是行，不是字节。
- 达到约 `batch_size × 0.8` 行就可能发出批次，最后一批是余数。
- Native 和 Ray 都有效。这是单机调试内存时最有用的旋钮。

## Ray 上 `into_batches` 是一个 task 边界

Native 上它只是流内重新组批。**Ray 上不是。** 分布式执行时它分两阶段：先让上游 task 在本地按 `batch_size` 组批，然后**物化**这些输出，按累计约 `0.8 × batch_size` 行打包成组，每组生成一个新的下游 task。

```text
上游 task ──本地组批──► 物化到 object store ──按 ~0.8n 行成组──► 新 task
```

三个后果：

1. **它会重切 partition。** 官方 docstring 的原话是 "splits or coalesces DataFrame to partitions of size `batch_size`"。`into_batches(1_000_000)` 在 Ray 上是合并 partition，`into_batches(16)` 是把 partition 打碎。
2. **有物化成本**，数据要落一次 object store。它是流式的、不是全局 barrier，但不免费。
3. **上游的 clustering 信息作废。** 之前 `repartition(N, key)` 建立的 key 共址，过了 `into_batches` 就不再成立，下游 join / groupby 会重新 shuffle。

所以在 Ray 上，`into_batches` 既是内存旋钮也是分区旋钮，不要在 map-only 链路里随手插很多个。

## UDF 的 `batch_size`

`@daft.func.batch(batch_size=N)` 的 `N` 是单批上限，不是保证值。UDF 吃到的批不会大于上游 morsel。

```text
上游 into_batches(16) + UDF batch_size=128  → 实际最多 16 行，模型吃不饱
上游 morsel=8192 + UDF batch_size=8        → UDF 再切细，合理
```

先定膨胀点之后的行批，再让 UDF `batch_size` ≤ 这个值。不要两个数字拧着来。它与 `enable_dynamic_batching` 同时打开会冲突。其余 UDF 参数见 [UDF](06-udf.md)。

## 动态批

```python
daft.context.set_execution_config(
    enable_dynamic_batching=True,
    dynamic_batching_strategy="auto",  # 或 latency_constrained
)
```

**默认关闭。** 打开后引擎按运行时反馈调批。生产调参时先把静态 morsel 跑稳，再评估动态批，不要两件事一起开。

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

**调小无效时：** pipeline 里有 blocking sink（清单见[架构](01-architecture.md)）。先改算法、减 key 基数、换 broadcast / 去 shuffle，再回头看 morsel。

morsel 过小会推高调度开销与尾部 task 数。不要从 131072 直接砍到 1，除非行已经是 MB 级。

## `maintain_order`

默认 `True`。保序会引入排序缓冲，部分算子更重。

不需要输出顺序时：

```python
daft.context.set_execution_config(maintain_order=False)
```

也可用环境变量 `DAFT_MAINTAIN_ORDER=false`（这个名字在白名单里）。

`write_parquet` 等 sink 可能强制关掉保序并向下传播。下游如果依赖行序，不要假设写出之后还保持输入顺序。

## 别忘了乘积

```text
partition        决定有多少个 task 同时跑
morsel           决定每个 task 一次推多少行
batch_size       决定 UDF 一次推理多少行（≤ 上游 morsel）
max_concurrency  决定有多少个模型实例 / 协程同时吃这些批
```

四个数字相乘才是在途字节。`download(max_connections)` 默认 32 且会顶掉 `S3Config`，是这里最容易被忽略的乘数，见[读写参数](07-io-config.md)。

## 操作顺序

1. 看 `explain()`，确认膨胀点前后没有意外的 blocking sink。
2. 只在膨胀点**之前**插 `into_batches`。
3. 显式写 `download(max_connections=...)`。
4. 仍 OOM 再降全局 `default_morsel_size`。
5. 内存安全但 CPU 低，再把批往上加，找吞吐拐点。
