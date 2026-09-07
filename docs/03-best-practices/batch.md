# Batch 与 Morsel

这一页只谈**单 task 内部**怎么控峰值内存。分布式并行度去看 [Partition](partition.md)；原理细节去看 [Morsel](../02-principles/morsel.md)。

## 三层行批

| 旋钮 | 作用域 | 单位 | 怎么设 |
|---|---|---|---|
| `default_morsel_size` | 全局，Swordfish 默认行批 | **行**，默认 131072 | `set_execution_config(default_morsel_size=N)` |
| `into_batches(n)` | 计划中某一点之后 | **行** | `df.into_batches(16)` |
| UDF `batch_size` | 单个 batch UDF | **行**，上限不是保证值 | `@daft.func.batch(batch_size=8)` |

Daft 不读 `DAFT_DEFAULT_MORSEL_SIZE` 环境变量；要用它驱动，得在应用入口自己读出来转进 `set_execution_config`。详见 [Morsel](../02-principles/morsel.md)。

`enable_dynamic_batching` 默认关。先把静态批跑稳，再评估动态批。

## 何时用 `into_batches`

在**单行会膨胀**的算子之前压批，而不是把全局 morsel 砍到同样小。

典型膨胀点：

- `download()` / 读对象存储字节
- 图像 decode / resize
- 解压、JSON 展开、`explode`
- 模型推理、embedding、大 tensor

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

scan 阶段行还很瘦（几十字节的 URL），用默认甚至较大的 morsel 没问题。危险从“瘦列变胖列”那一步开始。

`into_batches` 的规则：

- 参数必须 `> 0`，单位是行。
- best effort：大约到 `batch_size * 0.8` 就可能发出。
- 最后一批是余数。
- Native 和 Ray 都有效。

## `download()` 默认 32 会覆盖 S3Config

这是内存排查里最容易被忽略的乘数。

```text
S3Config.max_connections     默认 8，且是「每 IO 线程」
url.download(max_connections) 默认 32，且会顶掉上面的 8
```

```python
from daft.io import IOConfig, S3Config

daft.context.set_planning_config(
    default_io_config=IOConfig(
        s3=S3Config(
            region_name="us-west-2",
            max_connections=8,
            num_tries=25,
        )
    )
)

# 错误：不写 max_connections，每个 morsel 仍按 32 路拉对象
df = df.with_column("bytes", daft.col("url").url.download())

# 正确：显式压到与 S3Config / 内存预算一致
df = df.into_batches(8).with_column(
    "bytes",
    daft.col("url").url.download(max_connections=8),
)
```

在途字节 ≈ `min(batch 行数, max_connections)` × 单对象大小。16 行一批、每行 4 MB 图、32 路下载，一个 task 就能同时在途数百 MB，再乘并发 task 数。

对象存储限流、worker 内存爬升、decode 前 working set 陡增时，先把 `download(max_connections)` 从 32 降到 4～8，再动 morsel。

## 和 UDF `batch_size` 对齐

UDF 吃到的批不会大于上游 morsel / `into_batches`。

```text
上游 into_batches(16) + UDF batch_size=128  → 实际最多 16 行，模型吃不饱
上游 morsel=8192 + UDF batch_size=8        → UDF 再切细，合理
```

先定膨胀点之后的行批，再让 UDF `batch_size` ≤ 这个值。不要两个数字拧着来。

## 操作顺序

1. 看 `explain()`，确认膨胀点前后没有意外的 blocking sink。
2. 只在膨胀点前插 `into_batches`。
3. 显式写 `download(max_connections=...)`。
4. 仍 OOM 再降全局 `default_morsel_size`。
5. 内存安全但 CPU 低，再把批往上加，找吞吐拐点。
