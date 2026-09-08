# 配置决策

一页回答「这个作业该怎么配」。按 **禁区 → 读侧 → partition → actor → 批大小 → 写出** 的顺序走一遍，每步给公式和算例。机制推导在后续各页，这里只给决策。

```text
四个旋钮，管的不是同一件事
  partition          几个 task 并行、活摊到几台机器
  max_concurrency    几个模型实例常驻（@daft.cls）
  morsel / batch     单 task 一次推多少行 → 峰值内存
  download 连接数    同时在途多少个远端对象
```

调参顺序固定：**资源 → partition → morsel → UDF → I/O → 写出**。一轮只改一个变量，动手前存 `explain(show_all=True)`。

### 一分钟版

| 步 | 问什么 | 一句话答案 |
|---|---|---|
| [0](#零--先划禁区不要把数据拉回-driver) | 出口对不对 | 只能 `write_*`；大结果不许 `collect` / `to_pandas` / `iter_rows` |
| [1](#一--读侧先确认切出了多少-task) | 读侧切出几个 task | `print(df.num_partitions())`；小文件必开 `enable_scan_task_split_and_merge` |
| [2](#二--partition-怎么定) | partition 定多少 | map-only：`2 × 总 CPU`；有 `@daft.cls`：**≥ 节点数** |
| [3](#三--actor-资源与并行度怎么算) | actor 并行度定多少 | 一个节点放几个 × 节点数 × 0.8 |
| [4](#四--批大小控单-task-峰值内存) | 批大小定多少 | 膨胀点**之前** `into_batches(n)`，且 `batch_size ≤ n` |
| [5](#六--写出) | 写出怎么收 | 先 coalesce；`write_mode="overwrite"` |

---

## 零 · 先划禁区：不要把数据拉回 driver

DataFrame 是惰性的，`select` / `where` / `with_column` / `join` 只加计划节点。**大结果禁止以下出口**：

| 类别 | API |
|---|---|
| 全量物化 | `collect()`、`to_pandas()`、`to_arrow()`、`to_pydict()`、`to_pylist()`、`pa.table(df)` |
| driver 流式消费 | `iter_rows()`、`to_arrow_iter()`、`iter_partitions()` |
| 交给其他框架 | `to_torch_*`、`to_ray_dataset()`、`to_dask_dataframe()` |
| 跑整图只为一个数 | `count_rows()`、`count()` |

driver 跑在 head 上，内存按**调度规格**配、不按数据量配。`collect()` 一个 TB 级结果不是慢，是把分布式作业退化成单机拷贝然后 head OOM。

在链路中间插 `collect()` 更糟：它切断优化器，谓词下推和列裁剪一起失效。要分阶段就**写中间 sink，下一段重新 `read_*`**。

```python
# 错：中间物化，优化器被切断
rows = df.where(col("dt") == "2026-09-01").collect()

# 对：全程惰性，以 write_* 收尾
df.where(col("dt") == "2026-09-01").write_parquet("s3://bucket/stage1/")
```

合法终点只有 `write_parquet` / `write_lance` / `write_iceberg` / `write_csv` / `write_json`。`show(n)` 可以预览（只跑前 n 行），但它仍然启动执行，不要当巨大计划的免费探活。

上面这些出口只允许用在冒烟测试，和聚合后确定装得下 driver 的小表。逐条禁区见[生产禁区](09-production-donts.md)。

---

## 一 · 读侧：先确认切出了多少 task

入口并行度来自 **ScanTask**，不是 `into_partitions`。partition 只能重切已经读出来的数据，所以先看读侧。

```python
df = daft.read_parquet("s3://bucket/data/*.parquet")
print(df.num_partitions())      # 这才是起点
```

关键：`enable_scan_task_split_and_merge` **默认 `False`**，关着时**一个文件一个 task**。

| 参数 | 默认 | 作用 |
|---|---:|---|
| `enable_scan_task_split_and_merge` | **False** | 合并 / 切分总开关 |
| `scan_tasks_min_size_bytes` | 96 MB | 合并下限 |
| `scan_tasks_max_size_bytes` | 384 MB | 切分上限，超过就拆 |
| `max_sources_per_scan_task` | 10 | 单 task 最多打包几个文件 |
| `parquet_split_row_groups_max_files` | 10 | 文件数少于此值才按 row group 切 |

```python
daft.context.set_execution_config(
    enable_scan_task_split_and_merge=True,
    scan_tasks_min_size_bytes=96 * 1024 * 1024,
    scan_tasks_max_size_bytes=384 * 1024 * 1024,
    max_sources_per_scan_task=10,
)
```

诊断：

| `num_partitions()` 结果 | 原因 | 动作 |
|---|---|---|
| 等于文件数，且文件很小 | 开关没开 | 打开开关，必要时提高 `max_sources_per_scan_task` |
| 远小于 CPU 数，文件很少但很大 | 文件数超过 `parquet_split_row_groups_max_files`，整批不拆 | 检查该阈值；单 row group 巨型文件读侧拆不开 |
| Lance 上 task 数暴涨 | 一 fragment 一 task | 加 `fragment_group_size`，不是 `into_partitions` |

**这个 pass 只在 Ray 分布式翻译时执行**，Native 上改这些参数对 task 划分没有效果。参数细节见 [Partition](04-partition.md#scan-task入口-partition-的真正来源)。

---

## 二 · Partition 怎么定

### 选哪个 API

| 目的 | API | 代价 |
|---|---|---|
| 调 task 数 / 并行度 / 写出前 coalesce | `into_partitions(N)` | 便宜，机械拆合 |
| join / groupby 前按 key 共址 | `repartition(N, key)` | 贵，全局 shuffle |
| 随机打散倾斜 | `repartition(N)` | 贵 |
| 控单 task 峰值内存 | `into_batches(n)` | Ray 上会重切 partition |

只想改 task 数就别用 `repartition`。连续的 `repartition` / `into_partitions` 会被优化器折叠，**以最后一个为准**。

### 定多少：map-only 链路

无 `@daft.cls` 的读 → 变换 → 写：

```text
Ray 总 CPU      = worker 副本数 × 每副本 num-cpus
初始 partitions = 2 × 总 CPU
搜索点          = 1× / 2× / 4× 总 CPU
```

> **算例 A** — 8 个 worker × 8 CPU = **64 总 CPU**，读 S3 上 3 万个 2 MB 小文件。
>
> 1. 先开 scan 合并：3 万小文件按 96 MB 下限合并，`max_sources_per_scan_task=10` 限制单 task 最多 10 个文件 → 约 3000 个 scan task。
> 2. 3000 远大于 `2 × 64 = 128`，task 太碎、调度开销上升 → `into_partitions(128)` 合并。
> 3. sweep `64 / 128 / 256`，看 CPU 是否吃满、task P95 是否变长。

| 太少 | 太多 |
|---|---|
| worker CPU 空闲、单 task 太长、失败重算贵 | driver metadata 膨胀、调度开销上升、下游小文件多 |

### 定多少：有 `@daft.cls` 推理 / embed

**这里不能套 `2 × 总 CPU`，也不能用 `2 × max_concurrency`。** 两个参数管不同维度：

| 参数 | 管什么 |
|---|---|
| `into_partitions(N)` | 上游 Swordfish task 数 → Ray 把活摊到几个 worker |
| `max_concurrency` | 全集群 actor 实例数 → 模型池多大 |

执行时的三条事实（源码行为）：

1. 一个 partition 生成一个带 UDF 的 Swordfish task。
2. Actor 启动用 **SPREAD** 散布全集群。
3. **每个 task 只用本机 actor**——本节点有 actor 时不会去调远端的。

推论：**partition 数 ≈ 同时参与推理的 worker 数上限**。

```text
有效并行 ≈ 活跃 task 数 × 该节点本地 actor 数    ≠ max_concurrency
```

所以 partition 按「要让多少台机器干活」定，下限是节点数：

| 链路 | partition |
|---|---|
| 纯推理（数据已在 Lance，读很快） | **≥ 节点数**；太少会直接锁死参与的机器数 |
| 前面有 download / decode / scan | `1×～2× 总 CPU` |
| 写出前 | coalesce 到目标文件数，与计算 partition 分开 |

> **算例 B** — 2000 总 CPU、133 节点 × 15 CPU、embed 用 `@daft.cls(cpus=2)`，当前 `into_partitions(50)`。
>
> **症状**：只有约 50 台机器 CPU 高，其余节点有 UDFActor 但闲着。
>
> **原因**：50 partition → 约 50 个 task → 落到约 50 个 worker；每个 worker 只用本机 actor（750 个 SPREAD 到 133 节点 ≈ 5～6 个/节点）→ 有效并行约 300，其余 400 多个 actor 空转。
>
> **动作**：把 partition 提到 **≥ 133**，实际从 `500 / 1000 / 2000` sweep。`max_concurrency` 不动。

详细推导见 [Partition · actor 与 partition](04-partition.md#daftcls-与-partition两个维度)。

---

## 三 · Actor 资源与并行度怎么算

只对 `@daft.cls`（模型常驻）。`@daft.func` / `.batch` 是无状态的，不占常驻资源。

### 三个参数的含义

| 参数 | 含义 | 不写的默认 |
|---|---|---|
| `cpus` | **每实例**预留的 CPU | 按 **1.0** 提给 Ray，不是 0 |
| `gpus` | **每实例**预留的 GPU | 0；支持 0～1 小数，>1 必须整数 |
| `max_concurrency` | 同步 = **全集群 actor 数**；async = 每 worker 内协程数 | 无（不设不建 actor 池） |

`cpus` 取单实例真正会用到的线程数（tokenize、collate、拷贝），不是「想分多少」。Ray 只做放置记账、不做 cgroup 隔离——写大了只会让 `floor(单节点 CPU ÷ cpus)` 变小、白白降密度。

### 怎么算：先算一个节点，再乘节点数

```text
第 1 步   一个节点放几个 = floor( min( 节点 CPU ÷ cpus,
                                      节点 GPU ÷ gpus,
                                      节点可用内存 ÷ 每 actor RSS ) )

第 2 步   max_concurrency = 第 1 步 × 节点数 × 0.8
                                              └ 留给 scan / download / 写出 / Ray
```

三项取小：CPU 算出来能放 4 个、内存只够 2 个，就以内存为准——按 CPU 排满的结果是 worker 被 OOMKilled、actor 反复重启。「节点可用内存」= Pod memory limit − object store（`/dev/shm`）− 系统开销。

**别照 Daft 的预检查填。**它算的是 `floor(总 CPU ÷ cpus)`，不看单个节点放不放得下，所以给出的数常常偏大。按上面「单节点 × 节点数」算，才是实际能就绪的数量。

> **算例 C（CPU 模型，即算例 B 的集群）** — 133 节点 × 15 CPU、`cpus=2`、每 actor RSS 约 2.5 GiB、节点可用内存约 56 GiB。
>
> ```text
> 第 1 步   min(15÷2=7, 无 GPU, 56÷2.5=22) = 7 个/节点      ← CPU 是瓶颈
> 第 2 步   7 × 133 = 931，× 0.8 ≈ 750
> ```
>
> Daft 的预检查会给 `floor(2000÷2) = 1000`，但单节点只放得下 7 个，多出来的卡在 PENDING。**填 750，sweep 600 / 750 / 850。**

> **算例 D（GPU 模型）** — 2 个 worker，每个 8 CPU + 1 GPU，`gpus=1`。
>
> ```text
> 第 1 步   min(8÷cpus, 1÷1=1) = 1 个/节点                  ← GPU 是瓶颈
> 第 2 步   1 × 2 = 2  →  max_concurrency = 2
> ```
>
> GPU 作业基本总是 GPU 项说了算。此时 `cpus` 不改变总数，只决定单节点塞得下几个——可以放心抬到 **4**，让每个 actor 有足够核做预处理和 H2D 拷贝。注意 GPU actor 光写 `gpus=1`，`cpus` 不写也会各占 1 核。

### 配错了怎么表现

| 配错方式 | 表现 |
|---|---|
| `cpus` / `gpus` 大于单节点规格 | 直接 `RuntimeError`，能立刻看见 |
| `max_concurrency` 超过实际能放的 | 只有一条 warning（日志搜 `only ... actors can be scheduled`），然后**空等满 `actor_udf_ready_timeout`（默认 120 秒）**再带部分 actor 继续跑 |

第二种最容易被当成「Daft 慢」——启动后什么都不干整两分钟，其实是在等永远起不来的 actor。

### 异步 UDF 另算

```text
async 的 max_concurrency = 每个 class worker 内的并发协程数，不是 actor 数
真实在途量 = max_concurrency × class worker 数，再被 DAFT_MAX_ASYNC_UDF_INFLIGHT_TASKS（默认 64）截断
```

异步侧的真实并发不能从配置数字反推，以 `ray status`、Dashboard 的 Actors 页和实际占用 CPU 为准。需要更高在途量时显式加大那个环境变量，并同步看内存。

完整推导见 [UDF](05-udf.md)。

---

## 四 · 批大小：控单 task 峰值内存

partition 管并行度，**批大小管内存**，两者不能互相替代。

```text
在途内存 ≈ 同时在处理的批数 × 每批行数 × 每行实际大小
             │                 │            └ decode / 推理后的真实字节，可比 scan 大三个数量级
             │                 └ min(default_morsel_size, into_batches(n), batch_size)  取小，不相乘
             └ 并发 task 数 × UDF 实例数
```

`default_morsel_size` 默认 **131072 行**（行数不是字节）。这是窄表标量列的值，URL、blob、解码图像、embedding 继续用它，单批能到数 GB。

| 行形态 | 起点（行） |
|---|---|
| 窄表、标量列 | 1024 ～ 131072 |
| 大字符串、百 KB 级对象 | 16 ～ 64 |
| MB 级对象、解码结果 | 4 ～ 16 |
| 大 tensor / 重中间状态 | 1 ～ 8 |

三条配置纪律：

1. **`default_morsel_size` 只能经 `set_execution_config` 传入**。`DAFT_DEFAULT_MORSEL_SIZE` 环境变量 Daft 不读，设了不报错也不生效。
2. **`into_batches(n)` 必须插在膨胀算子之前**（download / decode / explode / 推理）。插在 decode 之后管不到 decode 本身。
3. **UDF `batch_size` ≤ 上游 `into_batches(n)`**。它是上限不是保证值，吃不到比上游 morsel 更大的批。别和 `enable_dynamic_batching` 同时开。

```python
df = (
    df.into_batches(16)                                    # 膨胀点之前
      .with_column("bytes", col("url").url.download(max_connections=8))
      .with_column("image", col("bytes").image.decode())
      .with_column("emb", embedder.encode(col("image")))   # batch_size ≤ 16
)
```

调小无效说明 pipeline 里有 blocking 算子（`sort` / `aggregate` / `distinct` / `window` / join build 侧 / 写出）——批大小要求穿不过它们。先改算法、减 key 基数，再回头动参数。机制见[执行模型](02-execution-model.md)。

Pod 总内存还要加**模型常驻 RSS、Object Store（`/dev/shm`）和 Ray 系统进程**，完整预算见[资源与调参](08-tuning-runbook.md)。所以只把 morsel 调小并不保证不 OOM——先判断涨的是哪一块。

---

## 五 · I/O 并发

`download(max_connections)` **必须显式写**。默认 32，且它会**覆盖** `S3Config.max_connections`——在 `S3Config` 里配 8 但 `download` 不写，实际按 32 跑，内存乘数被悄悄放大。

```python
io_config = IOConfig(s3=S3Config(max_connections=8, num_tries=25, retry_mode="adaptive"))
daft.context.set_planning_config(default_io_config=io_config)

df.with_column("bytes", col("url").url.download(max_connections=8))   # 必须写
```

总请求压力约等于 `并行 task 数 × 每 task 在途 IO × 连接池`——加 partition 会同时放大 I/O 压力。看到 503 / SlowDown / 连接超时就降并发，不要加重试风暴。完整参数见[读写参数](06-io-config.md)。

---

## 六 · 写出

**计算 partition ≠ 写出 partition。**并行度直接变成文件数：128 路 UDF 跑完立刻写出，就是约 128 个文件（再乘 `partition_cols` 基数）。

```python
df = df.into_partitions(128)    # 计算阶段要并行
# ... UDF / filter / project ...
df = df.into_partitions(16)     # 写出前 coalesce
df.write_parquet("s3://bucket/out/", write_mode="overwrite", write_success_file=True)
```

两个默认要注意：

- `write_mode` 默认 **`append`**，重跑只会追加新 UUID 文件，**不幂等**。重跑作业用 `overwrite` 或事务表。
- `partition_cols` 是**目录分区**，不是 task 分区。高基数列（`user_id`、`request_id`）会让每个分区值开一个 writer，内存和小文件一起爆。只放 `dt` / `hour` / `region` 这类低基数列。

写出语义细节见[读写参数](06-io-config.md)。

---

## 七 · 完整配置模板

```python
import os
import daft
from daft import DataType, col
from daft.io import IOConfig, S3Config

TOTAL_CPU = int(os.environ["TOTAL_CPU"])        # worker 副本数 × num-cpus
N_NODES = int(os.environ["N_NODES"])

daft.set_runner_ray()                            # RayJob 里 driver 已在集群内

# 1. IOConfig：构造 DataFrame 前就要定
io_config = IOConfig(
    s3=S3Config(
        region_name=os.getenv("S3_REGION", "us-east-1"),
        endpoint_url=os.getenv("S3_ENDPOINT"),   # 自建对象存储才需要
        key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        max_connections=8,
        num_tries=25,
        retry_mode="adaptive",
        connect_timeout_ms=30_000,
        read_timeout_ms=30_000,
        multipart_max_concurrency=16,            # 默认 100，过大吃内存
    )
)
daft.context.set_planning_config(default_io_config=io_config)

# 2. execution config：morsel、scan 切分、actor 超时的唯一生效入口
daft.context.set_execution_config(
    default_morsel_size=8192,
    enable_scan_task_split_and_merge=True,       # 默认 False，小文件场景必开
    scan_tasks_min_size_bytes=96 * 1024 * 1024,
    scan_tasks_max_size_bytes=384 * 1024 * 1024,
    max_sources_per_scan_task=10,
    actor_udf_ready_timeout=600,                 # 默认 120s，模型冷启动要放宽
)

# 3. UDF：资源和并发在类上，批大小在方法上（放错位置不报错，只是不生效）
@daft.cls(
    gpus=1,                                      # 必须 ≤ 单 worker 的 num-gpus
    cpus=4,
    max_concurrency=2,                           # 见 §三：min(总量, 碎片, 内存) 再留余量
    max_retries=2,                               # 重试一次调用，不是重启 actor
    on_error="raise",                            # log / ignore 会把失败行静默置 null
)
class Embedder:
    def __init__(self, model_path: str):
        self.model = load_model(model_path)

    @daft.method.batch(
        return_dtype=DataType.fixed_size_list(DataType.float32(), 768),
        batch_size=8,                            # ≤ 上游 into_batches
    )
    def encode(self, images: daft.Series):
        return self.model.encode(images.to_pylist())

embedder = Embedder("/models/clip")              # 懒初始化，执行时才进 __init__

# 4. 读：先确认读侧切出了 task（§一）
df = daft.read_parquet("s3://bucket/meta/*.parquet").select("id", "url")
print(df.num_partitions())

# 5. partition：map-only 用 2×CPU；有 actor 时至少 ≥ 节点数（§二）
df = df.into_partitions(max(N_NODES, 2 * TOTAL_CPU))

# 6. 批大小：压批在膨胀点之前（§四）
df = (
    df.into_batches(16)
      .with_column("bytes", col("url").url.download(max_connections=8))
      .with_column("image", col("bytes").image.decode())
      .with_column("emb", embedder.encode(col("image")))
)
print(df.explain(show_all=True))                 # 存下来，事后复盘要用

# 7. 写出：先 coalesce，overwrite 保证重跑幂等（§六）
df.into_partitions(16).write_parquet(
    "s3://bucket/out/", write_mode="overwrite", write_success_file=True,
)
```

`set_planning_config(default_io_config=...)` 定了之后，所有 `read_*` / `write_*` / `url.download()` 不显式传 `io_config` 就都用它。改完在 driver 里把 effective config 打出来确认，但**不要**把密钥打进日志或 `explain()` 输出。

---

## 八 · 配错了怎么看出来

| 现象 | 先看 | 动作 |
|---|---|---|
| CPU 低 + task 很少 | `df.num_partitions()` | 开 scan split/merge（§一）；再加 partition |
| 大量节点 idle、少数几十台在推理 | 推理前 partition 数、Ray 活跃 task 数 | 推理**前** `into_partitions` ≥ 节点数（§二）；别只加 `max_concurrency` |
| actor 一直 PENDING | `ray list actors` | 核对 `cpus` / `gpus` ≤ 单 worker 规格（§三 ①） |
| 启动空等 ~120 秒才开跑 | 日志搜 `only ... actors can be scheduled` | `max_concurrency` 配超了，按 §三 重算 |
| `OOMKilled` | working set、RSS、Object Store | 减 morsel / `into_batches` → 减 actor 并发 → 减 `download` 连接（§四） |
| actor 反复 `RESTARTING` | `ray list actors` | 先解决内存，**不是**加并发 |
| Object Store spill | spill 指标 | 加大 `/dev/shm` 或减 refs；大 shuffle 评估 Flight |
| task 变多但更慢 | task 数、文件数、GCS 延迟 | 减 partition、合并 scan task、compaction |
| 输出大量小文件 | 写出前 partition、`partition_cols` 基数 | 写出前 coalesce；降目录分区基数（§六） |
| 单 task 像在跑全量 | physical plan | 存 `explain(show_all=True)`，确认 runner 是 Ray、调用顺序对 |

**永远不要**为了「修 OOM」去关 Ray memory monitor（`RAY_memory_monitor_refresh_ms=0`）——那只是把软驱逐换成 `OOMKilled`。完整症状表见[资源与调参](08-tuning-runbook.md)。

### 三十秒体检

```bash
kubectl -n "$NS" exec -c ray-head "$HEAD" -- ray status                    # Total CPU 对不对
kubectl -n "$NS" get pods -l ray.io/cluster="$CLUSTER"                     # 有没有反复重启
kubectl -n "$NS" exec -c ray-head "$HEAD" -- \
  ray list actors --address http://127.0.0.1:8265                          # actor 稳不稳
```

```text
Total CPU 少于预期   有 worker 没起来，先查 Pending，别急着调参
RESTARTS 在涨        内存问题，现在就保留现场
actor RESTARTING     初始化在反复重做，等下去没有意义
```

排障方向**自下而上**：K8s → KubeRay → Ray → Daft。`kubectl logs` 只覆盖容器 stdout，Ray 真正的现场在 Pod 里的 `/tmp/ray`，**Pod 一重建就没了**——失败时先 `kubectl cp` 或 `ray job logs` 带走。日志路径和关键指标见[日志与监控](07-observability.md)。

---

## 九 · 其他默认陷阱

| 参数 | 默认 | 不配的后果 |
|---|---|---|
| `enable_scan_task_split_and_merge` | False | 一个文件一个 task，小文件切不出并行度 |
| `url.download(max_connections)` | 32 | 覆盖 `S3Config` 的值，内存乘数被放大 |
| `write_mode` | `append` | 重跑追加新文件，**不幂等** |
| `actor_udf_ready_timeout` | 120 秒 | 模型冷启动来不及，空等后带部分 actor 跑 |
| `maintain_order` | True | 不需要顺序时白付排序缓冲 |
| `flight_shuffle_dirs` | `["/tmp"]` | 容器 overlay 盘又慢又小，生产要挂本地盘 |
| `DAFT_MAX_ASYNC_UDF_INFLIGHT_TASKS` | 64 | 异步 UDF 在途上限，调高并发时容易撞它 |
| `DAFT_MEMORY_LIMIT` | cgroup 或宿主机总内存 | 只门控声明了 `memory_bytes` 的 UDF permit 池；cgroup 探测失败时会误用宿主机 RAM |
| `DAFT_ANALYTICS_ENABLED` | 开 | 内网 / 离线环境应设 `0` |

`head` 侧还有三条部署硬约束：`num-cpus: "0"`、每组 `requests == limits`、`num-cpus == limits.cpu`。见 [KubeRay 部署](03-deploy-kuberay.md)。

---

## 十 · 上线前勾一遍

- [ ] 以 `write_*` 收尾，路径上没有大结果 `collect` / `to_pandas` / `iter_rows` / `to_ray_dataset` 等 driver 出口
- [ ] runner 是 Ray，提交方式是 RayJob 或 Jobs API（不是 `ray://` Client）
- [ ] `num_partitions()` 打印过，读侧确实切出了 task
- [ ] partition 与 `max_concurrency` 分别算过；有 actor 时 partition ≥ 节点数
- [ ] `max_concurrency` 取了 `min(总量, 单节点碎片, 内存)` 并留了余量；日志里没有 `only ... actors can be scheduled`
- [ ] actor 资源 ≤ 单 worker 资源；CPU 没被 actor 填满
- [ ] `default_morsel_size` 经 `set_execution_config` 传入；`into_batches` 在膨胀点之前
- [ ] 显式写了 `download(max_connections)`
- [ ] 写出前 coalesce；`write_mode="overwrite"` 或事务表；`partition_cols` 只有低基数列
- [ ] head `num-cpus: "0"`；worker `requests == limits == num-cpus`
- [ ] 镜像是不可变 tag / digest；RayJob 设了 `activeDeadlineSeconds` / `ttlSecondsAfterFinished`
- [ ] 保存了 `explain(show_all=True)` 和 effective config；`/tmp/ray` 有采集

这一页解决不了的，按 [首页](index.md) 的导航往下找。
