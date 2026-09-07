# Partition

Partition 是 Flotilla 的工作单元：**一个 partition = 一个 task**。它决定并行度、单 task 重量、失败重算粒度和（如果写出前不合并）输出文件数。

它不解决单 task 峰值内存——那是 [执行模型](02-execution-model.md) 里 morsel / `into_batches` 的事。

## 四个 API

| API | 是否全局 shuffle | 改什么 | Native 上 |
|---|---|---|---|
| `into_partitions(N)` | 否，机械拆分 / 合并 | 只改 partition 数，保留上游 clustering | no-op |
| `repartition(N)` | 是，随机 shuffle | 打散负载，不保证 key 共址 | no-op |
| `repartition(N, key)` | 是，按 key 哈希 | 相同 key 落到同一 partition | no-op |
| `into_batches(n)` | 否，但**会物化并重切 partition** | 主要改行批；Ray 上顺带把 partition 切成 ~n 行 | **有效**（只改行批，不改 partition） |

`into_batches` 那一行容易踩坑。Native 上它是流内组批；**Ray 上分两阶段**：上游 task 本地组批 → 物化到 object store → 按 ~0.8n 行打包成新 task。三个后果：

1. **重切 partition**——`into_batches(1_000_000)` 合并 partition，`into_batches(16)` 打碎 partition。
2. **有物化成本**，数据落一次 object store（流式、非全局 barrier，但不免费）。
3. **上游 clustering 作废**——`repartition(N, key)` 建立的共址穿不过它，下游 join / groupby 会重新 shuffle。

map-only 链路里不要随手插很多个。想控单 task 内存用它没问题；并行度和 key 共址另算。

```python
# 扫描后只有 3 个 partition，希望 64 路并行 UDF
df = daft.read_parquet("s3://bucket/data/*.parquet")
df = df.into_partitions(64)

# 按 key 哈希共址（贵；join / groupby 前才需要）
df = df.repartition(128, "user_id")

# 写出前合并，减少小文件
df = df.into_partitions(8)
df.write_parquet("s3://bucket/out/", write_mode="overwrite")
```

决策：

```text
只调 task 数 / 文件数 / 并行度     → into_partitions(N)
按 key 共址 / join 前固定分布     → repartition(N, key)
随机打散倾斜（昂贵）               → repartition(N)
控单 task 峰值内存                 → into_batches(n)
```

`into_partitions` **不考虑**各 partition 原有字节数，只按 task 数 / 行数机械切。它不消除倾斜——大 partition 拆开后仍可能比别的大。连续 `repartition` / `into_partitions` 会被优化器折叠，以最后一个为准。

join / groupby / distinct / window 会自动插入 hash repartition。手动 `repartition` 不一定是最终分布，以 `explain(show_all=True)` 为准。

## Scan task：入口 partition 的真正来源

读侧先把文件列表切成 ScanTask，这才是整条 pipeline 的初始并行度。`into_partitions` 发生在这之后。

| 参数 | 默认 | 作用 |
|---|---:|---|
| `enable_scan_task_split_and_merge` | **False** | 合并 / 切分总开关。关着时一个文件一个 task |
| `scan_tasks_min_size_bytes` | 96 MB | 合并下限；拆分时每片累加下限 |
| `scan_tasks_max_size_bytes` | 384 MB | 切分上限：超过就拆 |
| `max_sources_per_scan_task` | 10 | 单个 task 最多打包几个文件 |
| `parquet_split_row_groups_max_files` | 10 | 文件数少于此值才按 row group 切 |
| `enable_multi_glob_path_tasks` | False | Ray 上并行列举路径 |
| `scantask_max_parallel` | 8 | 并发 scan 数，**仅 native runner** |

默认开关是关的。小文件多或大文件需要按 row group 切开时，必须显式打开：

```python
with daft.execution_config_ctx(
    enable_scan_task_split_and_merge=True,
    scan_tasks_min_size_bytes=96 * 1024 * 1024,
    scan_tasks_max_size_bytes=384 * 1024 * 1024,
    max_sources_per_scan_task=10,
):
    df = daft.read_parquet("s3://bucket/data/*.parquet")
    print(df.num_partitions())
```

经验：

- 大量小文件 → 打开开关，必要时提高 `max_sources_per_scan_task`。根治是离线 compaction。
- 少量大文件 → 确认文件数不超过 `parquet_split_row_groups_max_files`，否则整批不拆。单 row group 的巨型文件读侧拆不开。
- 两条路都走不通时，`into_partitions` 才是最后手段。它只能重新切已经读出来的数据，减不了小文件的元数据开销。
- 这个 pass **只在 Ray 分布式翻译时执行**。Native 上改这些参数对 task 划分没有效果。

拆分侧用的是**磁盘压缩字节**，合并侧用的是**估算内存字节**（再乘 inflation factor）。同一个 96 / 384 在两个阶段量纲不同。

## 起点公式

```text
Ray 总 CPU      = worker 副本数 × 每副本 num-cpus
初始 partitions = 2 × 总 CPU
搜索点          = 1× / 2× / 4× 总 CPU
```

例：8 个 worker × 8 CPU = 64 总 CPU。从 64 / 128 / 256 三个点看拐点。

| 太少 | 太多 |
|---|---|
| worker CPU 空闲 | Ray task / GCS 元数据膨胀 |
| 单 task 太长 | ObjectRef 与调度开销上升 |
| 失败重算成本大 | 下游小文件增多 |

输入文件很少或很小时，按字节自动切分切不出足够 task，必须显式 `into_partitions`，否则并行度被输入形状卡死。

同时观察：CPU 是否吃满、task P50 / P95、单 worker 峰值内存、driver metadata、spill、输出小文件数。

## 计算 partition ≠ 写出 partition

并行度直接变成文件数。64 路 UDF 跑完立刻 `write_parquet`，就会写出约 64 个文件（再乘 `partition_cols` 的基数）。

```python
df = df.into_partitions(128)          # 计算阶段要并行
# ... UDF / filter / project ...
df = df.into_partitions(16)           # 写出前 coalesce
df.write_parquet(
    "s3://bucket/out/",
    write_mode="overwrite",
    write_success_file=True,
)
```

`partition_cols` 是目录分区，不是 task 分区。高基数（user_id、request_id）会让每个分区值开一个 writer，内存和小文件一起爆。目录分区只放低基数列：`dt`、`hour`、`region`。

## Lance：`fragment_group_size`

Lance 侧的 partition 粒度参数，对应 Parquet 的 `scan_tasks_*`。

```python
df = daft.read_lance(
    "s3://bucket/ds.lance",
    fragment_group_size=5,
    include_fragment_id=True,   # 后续 mode="merge" 必须开
)
```

`None` 或 `<= 1` 时一 fragment 一 task。fragment 碎、每个 task 只读一个 → task 数暴涨、driver metadata 压力上来。这时先加 `fragment_group_size`，而不是盲目 `into_partitions`。

写出后看返回 DataFrame 的 `num_small_files`：偏高就是碎片化信号。根治靠 `max_rows_per_file` 和 compaction，不是再拆一次 partition。

## Shuffle 边界

map-only 链路（读 → 变换 → 写）没有 shuffle。一旦出现 join / groupby / sort / `repartition`，先看 `M × N` 有多大。

| 算法 | 数据面 | 适用 |
|---|---|---|
| `auto` | 主要在前两者间选 | 默认起点，**不会自动切 Flight** |
| `map_reduce` | Ray Object Store | 中小规模 |
| `pre_shuffle_merge` | 先合并再走 Object Store | 输入 partition 很碎 |
| `flight_shuffle` | 本地盘 + Arrow Flight | > ~10 GiB 或 slot 矩阵很大 |

`flight_shuffle_dirs` 默认 `["/tmp"]`。容器里 `/tmp` 通常是 overlay 盘，生产必须显式挂本地盘。不要把 Flight 目录放在 overlay `/tmp` 上。
