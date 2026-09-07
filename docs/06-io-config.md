# 读写参数配置

I/O 参数分三层：planning 默认的 `IOConfig`、每次 `read_*` / `write_*` 的 API 参数、execution config 里的文件形态参数。改了没生效时，先确认改的是哪一层。

## 默认 IOConfig

所有 `read_*` / `write_*` 不显式传 `io_config` 时，落到 planning 层的默认值。这是唯一一个在构造 DataFrame 时就要定的性能参数。

```python
import daft
from daft.io import IOConfig, S3Config

daft.context.set_planning_config(
    default_io_config=IOConfig(
        s3=S3Config(
            region_name="us-west-2",
            max_connections=8,          # 每 IO 线程
            num_tries=25,
            retry_mode="adaptive",
            connect_timeout_ms=30_000,
            read_timeout_ms=30_000,
            multipart_size=8 * 1024 * 1024,
            multipart_max_concurrency=16,
        )
    )
)
```

自建对象存储还要配 `endpoint_url`、`force_virtual_addressing`、`verify_ssl`。密钥走环境或集群 Secret，不要写进镜像或 `explain()` 输出。

`set_planning_config` 只透传 `default_io_config` 和 `enable_strict_filter_pushdown`。改了必须在 driver 里把 effective `IOConfig` 打出来。

## S3Config 要点

| 参数 | 默认 | 注意 |
|---|---:|---|
| `max_connections` | 8（**每 IO 线程**） | 总连接 ≈ 8 × IO 线程数 × 并发 task |
| `num_tries` | 25 | 含首次 |
| `retry_mode` | `adaptive` | 另有 `standard` |
| `connect_timeout_ms` / `read_timeout_ms` | 30000 | 大对象或跨区要加大 |
| `multipart_size` | 8 MB | 写，范围 5MB～5GB |
| `multipart_max_concurrency` | 100 | 单对象分片并发，过大吃内存 |

Ray runner 上 `read_parquet(_multithreaded_io=False)` 可减少每 worker 连接与线程争用。不传时 Native 默认 True、Ray 默认 False。

## `download()` 默认 32 会覆盖 S3Config

这是内存排查里最容易被忽略的乘数。

```text
S3Config.max_connections     默认 8，且是「每 IO 线程」
url.download(max_connections) 默认 32，且会顶掉上面的 8
```

```python
# 错误：不写 max_connections，每个 morsel 仍按 32 路拉对象
df = df.with_column("bytes", daft.col("url").url.download())

# 正确：显式压到与 S3Config / 内存预算一致
df = df.into_batches(8).with_column(
    "bytes",
    daft.col("url").url.download(max_connections=8),
)
```

下载这一段占的内存 ≈ `min(batch 行数, max_connections)` × 单对象大小。16 行一批、每行 4 MB 图、32 路下载，一个 task 就能同时压着数百 MB，再乘并发 task 数。

对象存储限流、worker 内存爬升、decode 前 working set 陡增时，先把 `download(max_connections)` 从 32 降到 4～8，再动 morsel。压批位置见[执行模型](02-execution-model.md)。

## Parquet 读

`enable_scan_task_split_and_merge` 等属于 execution config，不是 `read_parquet` 的参数。见 [Partition](04-partition.md)。

| 参数 | 默认 | 说明 |
|---|---|---|
| `path` | 必填 | 本地、glob、目录、`s3://` / `gs://` / `hf://` |
| `row_groups` | `None` | 仅不含 glob 的显式文件列表可用 |
| `infer_schema` | `True` | `False` 时必须给 `schema`；文件多时推断有额外 I/O |
| `schema` | `None` | definitive 或 type hint |
| `io_config` | session 默认 | 认证与连接 |
| `file_path_column` | `None` | 追加来源路径，便于 checkpoint key |
| `hive_partitioning` | `False` | 从 `key=value/` 解析分区列 |
| `coerce_int96_timestamp_unit` | `None`（按 ns） | 旧 INT96 转为 `ns` / `us` / `ms` |
| `ignore_corrupt_files` | `False` | 只跳过格式损坏；不跳过网络 / 权限；会关掉 count 下推 |
| `checkpoint` | `None` | **仅 Ray**；Native 直接报错 |

```python
df = daft.read_parquet(
    "s3://bucket/events/dt=2026-09-03/*.parquet",
    hive_partitioning=True,
    file_path_column="source_path",
    _multithreaded_io=False,
)
```

`ignore_corrupt_files` 后通过 `df.skipped_corrupt_files` 看跳过列表，必须在 `collect()` 或写出触发 materialize 之后读取；`count_rows()` 不会填充该属性。

## Parquet 写

```python
written = df.write_parquet(
    "s3://bucket/output/",
    compression="zstd",
    write_mode="overwrite",
    write_success_file=True,
    partition_cols=["dt"],
    column_compression={"embedding": "lz4"},
)
```

| 参数 | 默认 | 风险 |
|---|---|---|
| `write_mode` | `append` | 重跑会新增 UUID 文件，**不幂等** |
| `write_success_file` | `False` | 成功写 `_SUCCESS`；不能替代行数对账 |
| `partition_cols` | `None` | 每个分区值一个 writer；高基数 = 内存 × 小文件 |
| `single_file` | `False` | **仅 Native**；不可与 `partition_cols` / `overwrite-partitions` 组合 |
| `compression` | `snappy` | 支持 snappy / gzip / zstd / lz4 / brotli / uncompressed |

`write_mode`：

| 值 | 行为 | 生产 |
|---|---|---|
| `append` | 只追加，不删旧文件 | 重跑作业不要用 |
| `overwrite` | 写完后删除 `root_dir` 下旧文件 | 整目录重跑用这个 |
| `overwrite-partitions` | 只覆盖本次涉及的分区目录 | 必须给 `partition_cols` |

裸文件目录不是事务表。失败可能残留部分文件；`overwrite` 也不等于全链路事务。需要原子提交时评估 Iceberg 等表格式。

execution config（不在 `write_parquet` 签名里）：

| 参数 | 默认 | 作用 |
|---|---:|---|
| `parquet_target_filesize` | 512 MB | 目标单文件大小，不是硬保证 |
| `parquet_target_row_group_size` | 128 MB | row group 内存缓冲 |
| `parquet_inflation_factor` | 3.0 | 内存 / 文件体积估算比 |
| `native_parquet_writer` | `True` | Rust writer |

inflation factor 不改真实内存，只改切分点估算。估偏了会出现小文件或超大文件，不是 OOM 的根因。

`partition_cols` 只放低基数列。`user_id`、`request_id` 当目录分区会把 writer 数打到和基数一样多。

## Lance 读

```python
df = daft.read_lance(
    "s3://bucket/ds.lance",
    version=3,
    fragment_group_size=5,
    include_fragment_id=True,
)
```

| 参数 | 说明 |
|---|---|
| `fragment_group_size` | 一个 scan task 打包几个 fragment；`None` / `<=1` 则一 fragment 一 task |
| `version` / `asof` | 读指定版本或时间点，重跑可复现 |
| `index_cache_size` | 默认 256 页，向量检索才相关 |
| `metadata_cache_size_bytes` | 元数据缓存上限 |
| `include_fragment_id` | `mode="merge"` 写回时必须为 True |

fragment 过碎时先加 `fragment_group_size`，再考虑 compaction。

## Lance 写

```python
result = df.write_lance(
    "s3://bucket/ds.lance",
    mode="overwrite",
    max_rows_per_file=1_000_000,
    max_rows_per_group=1024,
)
```

| `mode` | 含义 |
|---|---|
| `create` | 新建 |
| `append` | 追加 fragment |
| `overwrite` | 覆盖数据集 |
| `merge` | 只加列不重写数据，需要 join key，读侧带 `fragment_id` |

`**kwargs` 透传 `lance.write_fragments`：`max_rows_per_file`、`max_bytes_per_file`、`max_rows_per_group`、`data_storage_version`。

返回值是 DataFrame，不是 `None`。关注 `num_fragments` / `num_small_files` / `version`。`num_small_files` 偏高 = partition 太多或每 task 数据太少。

特征回填优先 `mode="merge"`，比重写整表便宜一个量级。

## 写出前的布局

1. 计算阶段用足够的 partition 吃满 CPU。
2. 写出前 `into_partitions` 收敛文件数。
3. `partition_cols` 只放低基数。
4. 重跑用 `overwrite` 或 `overwrite-partitions`，不用 `append`。
5. 打开 `write_success_file`，但仍要对账输入 / 输出行数。
6. Ray 上不要用 `single_file=True`，它只支持 Native。
