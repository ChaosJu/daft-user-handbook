# 速查一页

90% 的日常问题在这一页。这里只给**结论**和**去哪看推导**，机制都在后面九页里，不在这里重复。

## 十条铁律

1. 生产终点只有 `write_*`。大结果禁止一切把数据拉回 driver 的出口——见[第一节完整清单](#一--为什么不能-collect)。
2. **并行度调 partition，单 task 峰值内存调 morsel。**两者不能互相替代。
3. **峰值内存 ≈ 同时在处理的批数 × 每批行数 × 每行实际大小**，算的是"此刻正在处理的数据"吃多少内存，与数据总量无关。四个参数各管其中一项，拆解见[第二节](#二--三层数据单位先分清再调)。只压一项，另一项会把它乘回来——这就是"morsel 都调小了还是 OOM"的来源。
4. head `num-cpus: "0"`；每个组 `requests == limits`，且 `num-cpus == limits.cpu`。
5. 容器里必须设 `DAFT_MEMORY_LIMIT`，否则 Daft 按宿主机 RAM 做预算。
6. `default_morsel_size` **只能**经 `set_execution_config` 传入。`DAFT_DEFAULT_MORSEL_SIZE` 环境变量 Daft 不读，设了不报错也不生效。
7. `into_batches` 要插在膨胀算子（download / decode / explode / 推理）**之前**，插在后面对它无效。
8. `download(max_connections)` 必须显式写。默认 32，且会顶掉 `S3Config.max_connections=8`。
9. 重跑作业用 `write_mode="overwrite"`；`partition_cols` 只放 `dt` / `hour` / `region` 这类低基数列。
10. 调参从上往下（资源 → partition → morsel → UDF → I/O → 写出），一轮只改一个变量，动手前先存 `explain(show_all=True)`。

## 一 · 为什么不能 `collect`

DataFrame 是惰性的，`select` / `where` / `join` 只加计划节点。**大结果禁止这四类出口**，它们都会把数据（或等价全量）拉回 driver：

| 类别 | API |
|---|---|
| 全量物化 | `collect()`、`to_pandas()`、`to_arrow()`、`to_pydict()`、`to_pylist()`、`pa.table(df)` |
| 从 driver 流式消费 | `iter_rows()`、`to_arrow_iter()`、`iter_partitions()` |
| 交给其他框架 | `to_torch_map_dataset()` / `to_torch_iter_dataset()` / `to_torch_dataloader()`、`to_ray_dataset()`、`to_dask_dataframe()` |
| 跑整图只为一个数 | `count_rows()`、`count()` |

`show(n)` 只跑前 n 行，可以预览，但仍启动执行，别当巨大计划的免费探活。合法终点只有 `write_parquet` / `write_lance` / `write_iceberg` / `write_csv` / `write_json`。

driver 在 head 上，内存按**调度规格**配、不按数据量配——`collect()` 一个 TB 级结果不是慢，是把分布式作业退化成单机拷贝然后 head OOM。中间插 `collect()` 更糟：切断优化器，谓词下推和列裁剪一起失效，要分阶段就写中间 sink、下一段重新 `read_*`。

这些出口只允许用在冒烟测试、和聚合后确定装得下 driver 的小表。逐条禁区见[生产禁区](09-production-donts.md)。

## 二 · 三层数据单位，先分清再调

```text
1 个 DataFrame  =  N 个 partition       ← into_partitions(N) / repartition(N)
1 个 partition  =  1 个 Ray task        ← 并行度、重算粒度、写出文件数
1 个 task       =  很多 morsel 依次流过  ← default_morsel_size / into_batches(n)
1 个 morsel     ≥  1 个 UDF batch       ← batch_size 吃不到比上游 morsel 更大的批
```

**partition 是横向的**（几个 task 并行跑），**morsel 是纵向的**（一个 task 内部怎么流）。所以加 partition 不会让单个 task 少占内存，压小 morsel 也不会提高并行度。

Daft 是流式执行，不会把整个 partition 攒在内存里。所以内存不由**数据总量**决定，只由**同一时刻正在处理的那部分数据**决定（下文称"在途数据"）。partition、morsel、`batch_size`、`max_concurrency` 这四个参数不是直接相乘，它们各自落在下面三个因子上：

```text
在途数据占的内存 ≈ 同时在处理的批数 × 每批行数 × 每行实际大小

同时在处理的批数   每节点并发 task 数（partition 定上限）× UDF 实例数（max_concurrency）
每批行数           min(default_morsel_size, into_batches(n), UDF batch_size)   ← 取小，不是相乘
每行实际大小       download / decode / 推理之后的真实字节，可比 scan 时大三个数量级
```

`morsel` 和 `batch_size` 都是**行数**，后者是前者的再切分，所以取小而不是相乘；`download(max_connections)` 会在下载那一段再截一次同时在途的对象数。数据量翻十倍而在途数据不变时，内存占用可以不变——这就是流式执行的意义。

这个式子只算"在途数据"这一块。Pod 实际吃的内存还要加上**模型常驻 RSS、Ray object store（`/dev/shm`）和 Ray 系统进程**，完整预算公式见[资源与调参](08-tuning-runbook.md)。所以只把 morsel 调小并不保证不 OOM——得先判断涨的是哪一块。

| | Partition | Morsel / batch |
|---|---|---|
| 所在层 | Flotilla（跨 worker） | Swordfish（单 task 内） |
| 影响 | task 数、并行度、shuffle、重算粒度、文件数 | 峰值内存、单次处理开销、UDF 单批大小 |
| Native runner 上 | 基本 no-op | **有效** |

## 三 · 分区怎么用好

### 选哪个 API

```text
只调 task 数 / 文件数 / 并行度   →  into_partitions(N)     便宜，机械拆合
按 key 共址 / join 前定分布      →  repartition(N, key)    贵，全局 shuffle
随机打散倾斜                     →  repartition(N)         贵
控单 task 峰值内存               →  into_batches(n)        Ray 上会重切 partition
```

只想改 task 数就别用 `repartition`。`into_batches` 在 Ray 上有三个副作用要记住：会重切 partition、有一次 object store 物化成本、上游 `repartition(N, key)` 建立的共址会作废。详见 [Partition](04-partition.md)。

### 定多少个

```text
Ray 总 CPU      = worker 副本数 × 每副本 num-cpus
初始 partitions = 2 × 总 CPU
搜索点          = 1× / 2× / 4× 总 CPU
```

| 太少 | 太多 |
|---|---|
| worker CPU 空闲、单 task 太长、失败重算贵 | driver metadata 膨胀、调度开销上升、下游小文件多 |

### task 数不对时先查读侧

入口并行度来自 ScanTask，不是 `into_partitions`。`enable_scan_task_split_and_merge` **默认是关的**，关着时一个文件一个 task。

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

上面是临时探查用的作用域写法；生产在入口用 `set_execution_config` 全局定一次，见[下面的骨架](#九--可以直接抄的骨架)。

小文件多 → 打开开关，根治靠离线 compaction。Lance 侧对应参数是 `fragment_group_size`。`into_partitions` 是最后手段，它减不了小文件的元数据开销。

### 计算分区 ≠ 写出分区

并行度直接变成文件数：64 路 UDF 跑完立刻写出，就是约 64 个文件（再乘 `partition_cols` 基数）。

```python
df = df.into_partitions(128)   # 计算阶段吃满 CPU
# ... UDF / filter / project ...
df = df.into_partitions(16)    # 写出前 coalesce
df.write_parquet("s3://bucket/out/", write_mode="overwrite", write_success_file=True)
```

join / groupby / distinct / window 会自动插 hash repartition，手动 `repartition` 不一定是最终分布——以 `explain(show_all=True)` 为准。

## 四 · Morsel 起点

`default_morsel_size` 默认 **131072 行**，单位是行不是字节。这是窄表标量列的默认值，URL、blob、解码图像、embedding 继续用它，单批能到数 GB。

| 行形态 | 起点（行） |
|---|---:|
| 窄表、标量列 | 1024 ～ 131072 |
| 大字符串、百 KB 级对象 | 16 ～ 64 |
| MB 级对象、解码结果 | 4 ～ 16 |
| 大 tensor / 重中间状态 | 1 ～ 8 |

调小无效，说明 pipeline 里有 blocking 算子（`sort` / `aggregate` / `distinct` / `window` / join build 侧 / 写出）。这时先改算法、减 key 基数，再回头动参数。机制见[执行模型](02-execution-model.md)。

## 五 · UDF 速查

`@daft.udf` 0.7 废弃、**0.8 移除**，现在只有三个：

```python
@daft.func                # 逐行标量
@daft.func.batch(...)     # 批处理，拿到 Series，必须给 return_dtype
@daft.cls(...)            # 有状态、模型常驻，方法上配 @daft.method
```

七个真正需要决策的参数，其余（`unnest` / `use_process` / `ray_options` / `name_override`）保持默认即可：

| 参数 | 默认 | 线上配置建议 |
|---|---|---|
| `cpus` / `gpus` | `None`（按 **1.0** 提给 Ray）/ `0` | 每**实例**资源，只影响放置。GPU 模型写 `gpus=1`，且必须 ≤ 单 worker 的 `num-gpus`，否则 actor 永远 PENDING。小模型共卡才用 0～1 的小数，大于 1 必须整数 |
| `max_concurrency` | `None` | **同步 = actor 进程数，异步 = 协程并发数。**取 `min(按 GPU 能放的, 按 CPU 能放的, 按内存能放的)`，再给 I/O 和 Ray 留 1～2 核。同步 `@daft.func` 上设会直接 `ValueError` |
| `batch_size` | `None` | 单批行数**上限**，不是保证值。先定膨胀点后的 `into_batches(n)`，再让它 ≤ n；MB 级行取 4～16。别和 `enable_dynamic_batching` 同时开 |
| `return_dtype` | `func.batch` 必填 | 一律显式写。`method*` 虽能从类型 hint 推导，但推错要到执行期才暴露 |
| `max_retries` | `None`（= 0） | 重试**一次调用**（一行或一批），不是重启 actor。调外部 API 给 2～3 并确认幂等，否则重试会造成重复副作用 |
| `on_error` | `raise` | 生产保持 `raise`。改 `log` / `ignore` 必须同时上 null 率门禁——否则失败行变 null，作业照样"成功" |
| `actor_udf_ready_timeout` | **120 秒** | 经 `set_execution_config` 设，给模型真实加载时间的 2～3 倍。RayJob 每次冷启动都会撞它 |

资源和并发写在 `@daft.cls` 上，`batch_size` / `return_dtype` 写在方法装饰器上——放错位置不会报参数错，只是不生效。可运行的写法见[骨架](#九--可以直接抄的骨架)。

两个必须同时满足的不等式：

```text
① 单 actor 资源 ≤ 单 worker 资源     违反 → actor 永远 PENDING，作业静默卡死
② Σactor + 普通 task + 系统进程 ≤ 集群总量    actor 常驻，占的核拿不回来
最终 actor 数 = min(按 GPU 能放的, 按 CPU 能放的, 按内存能放的)，再留 1～2 核给 I/O 和 Ray
```

CPU 算出来能放 4 个、内存只够 2 个，就以内存为准。完整推导见 [UDF](05-udf.md)。

### `cpus` 与 `max_concurrency` 怎么从集群资源反推

两者含义不同：`cpus` 是**每实例**预留，`max_concurrency` 是**全集群**实例总数，相乘才是这个 UDF 从集群长期挖走的核。

```text
上限 N = min( 集群总GPU ÷ gpus,            # 有 GPU 时基本总是这一项说了算
              集群总CPU ÷ cpus,            # Daft 就是用这个公式做预检查
              单worker可用内存 ÷ 每actor RSS × worker 数 )
max_concurrency = N 再往下砍，留 1～2 核给 download / decode / 写出 / Ray 自身
```

16 CPU + 2 GPU 的集群跑 `gpus=1`：GPU 项给出 2，CPU 项给出 `16 ÷ cpus`，取小就是 **2**。此时 `cpus` 不改变总数，只决定单节点塞得下几个——所以可以放心把它抬到 4，让每个 actor 有足够核做预处理和 H2D 拷贝。纯 CPU 作业反过来，由 `总CPU ÷ cpus` 决定，26 核配 `cpus=2` 最多 13 个。

`cpus` 取值就取单实例真正会用到的线程数（tokenize、collate、拷贝），不是"想分多少"。Ray 只做放置记账、不做 cgroup 隔离，写大了只会让 `floor(单worker CPU ÷ cpus)` 变小、白白降密度。不写则按 1.0 预留，GPU actor 光写 `gpus=1` 也会各占 1 核。

**配超了不报错。** Daft 按集群总量算出能放几个，超了只打一条 warning，然后卡满 `actor_udf_ready_timeout`（默认 120 秒什么都不干）再带着部分 actor 继续跑。日志里搜 `only ... actors can be scheduled`。真正会硬报错的只有不等式 ①——单个节点都满足不了 `cpus` / `gpus`。

## 六 · 不配就出事的默认值

| 参数 | 默认 | 不配的后果 |
|---|---|---|
| `DAFT_MEMORY_LIMIT` | 按系统总内存 | 容器里读到宿主机 RAM，一路放行到 `OOMKilled` |
| `url.download(max_connections)` | 32 | 顶掉 `S3Config` 的 8，内存乘数被悄悄放大 |
| `default_io_config` | 全默认 | 对象存储并发、重试、超时全是默认值 |
| `write_mode` | `append` | 重跑只加新 UUID 文件，**不幂等** |
| `enable_scan_task_split_and_merge` | False | 一个文件一个 task，小文件场景切不出并行度 |
| `maintain_order` | True | 不需要顺序时白付排序缓冲 |
| `DAFT_MAX_ASYNC_UDF_INFLIGHT_TASKS` | 64 | 异步 UDF 同时在途调用数的隐式上限，调高并发时容易撞它 |
| `DAFT_ANALYTICS_ENABLED` | 开 | 内网 / 离线环境应设 `0` |
| `flight_shuffle_dirs` | `["/tmp"]` | 容器 overlay 盘又慢又小 |

## 七 · 症状 → 动作

| 现象 | 先看 | 动作 |
|---|---|---|
| CPU 低 + task 很少 | `df.num_partitions()` | 加 partition；打开 scan split/merge |
| CPU 低 + actor 一直 PENDING | `ray list actors` | 核对 actor `cpus` ≤ worker `num-cpus` |
| CPU 被 throttling | pod spec | `num-cpus` 与 CPU limit 对齐，别超卖 |
| working set 逼近 limit / `OOMKilled` | working set、RSS、Object Store | 先减 morsel / `into_batches`，再减 actor 并发和 `download` 连接 |
| actor 反复 `RESTARTING` | `ray list actors` | 先解决内存，**不是**加并发 |
| Object Store spill | `ray_object_store_memory{Location=SPILLED}` | 加大 `/dev/shm` 或减 refs；大 shuffle 评估 Flight |
| task 变多但更慢 | task 数、文件数、GCS 延迟 | 减 partition、合并 scan task、compaction |
| 输出大量小文件 | 写出前 partition、`partition_cols` 基数 | 写出前 coalesce；降目录分区基数 |
| 长尾明显 | task duration 分布 | 加 partition、拆 source、查 key skew |
| 单 task 像在跑全量 | physical plan | 存 `explain(show_all=True)`，确认 runner 是 Ray、调用顺序对 |

**永远不要**为了"修 OOM"去关 Ray memory monitor（`RAY_memory_monitor_refresh_ms=0`）——那只是把软驱逐换成 `OOMKilled`。完整症状表见[资源与调参](08-tuning-runbook.md)。

## 八 · 三十秒体检

```bash
kubectl -n "$NS" exec -c ray-head "$HEAD" -- ray status          # Total CPU 对不对
kubectl -n "$NS" get pods -l ray.io/cluster="$CLUSTER"           # 有没有在反复重启
kubectl -n "$NS" exec -c ray-head "$HEAD" -- \
  ray list actors --address http://127.0.0.1:8265                # actor 稳不稳
```

```text
Total CPU 少于预期   有 worker 没起来，先查 Pending，别急着调参
RESTARTS 在涨        内存问题，现在就保留现场
actor RESTARTING     初始化在反复重做，等下去没有意义
```

排障方向是**自下而上**：K8s → KubeRay → Ray → Daft。下层没起来，上层指标不会产生。`kubectl logs` 只覆盖容器 stdout，Ray 真正的现场在 Pod 里的 `/tmp/ray`，**Pod 一重建就没了**，失败时先 `kubectl cp` 或 `ray job logs` 带走。

## 九 · 可以直接抄的骨架

```python
import os

import daft
from daft import DataType
from daft.io import IOConfig, S3Config

TOTAL_CPU = int(os.environ["TOTAL_CPU"])       # worker 副本数 × num-cpus

daft.set_runner_ray()                          # RayJob 里 driver 已在集群内

# 1. IOConfig：构造 DataFrame 前就要定。密钥走环境或 Secret，不要写进镜像
io_config = IOConfig(
    s3=S3Config(
        region_name=os.getenv("S3_REGION", "us-east-1"),
        endpoint_url=os.getenv("S3_ENDPOINT"),          # 自建对象存储才需要
        key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        force_virtual_addressing=False,                 # MinIO 等路径寻址服务保持 False
        verify_ssl=True,
        max_connections=8,                              # 每 IO 线程
        num_tries=25,
        retry_mode="adaptive",
        connect_timeout_ms=30_000,
        read_timeout_ms=30_000,
        multipart_size=8 * 1024 * 1024,                 # 写侧分片
        multipart_max_concurrency=16,                   # 默认 100，过大吃内存
    )
)
daft.context.set_planning_config(default_io_config=io_config)

# 2. execution config：morsel、scan 切分、actor 超时都在这里，是唯一生效入口
daft.context.set_execution_config(
    default_morsel_size=8192,
    enable_scan_task_split_and_merge=True,              # 默认 False，小文件场景必开
    scan_tasks_min_size_bytes=96 * 1024 * 1024,
    scan_tasks_max_size_bytes=384 * 1024 * 1024,
    max_sources_per_scan_task=10,
    actor_udf_ready_timeout=600,                        # 默认 120s，模型冷启动要放宽
)


# 3. UDF：模型常驻用 @daft.cls，资源和并发在类上、批大小在方法上
#    这里假设 2 个 GPU worker，每个 8 CPU + 1 GPU（合计 16 CPU / 2 GPU）
@daft.cls(
    gpus=1,                                    # 每实例 1 卡，必须 ≤ 单 worker 的 num-gpus
    cpus=4,                                    # 每实例 CPU；不写默认按 1 预留，不是 0
    max_concurrency=2,                         # 同步方法 = 全集群 actor 进程数，这里被 GPU 总数卡死
    max_retries=2,                             # 重试一次调用，不是重启 actor
    on_error="raise",                          # log / ignore 会把失败行静默置 null
)
class Embedder:
    def __init__(self, model_path: str):
        self.model = load_model(model_path)    # 你自己的加载逻辑，慢就调大上面的 timeout

    @daft.method.batch(
        return_dtype=DataType.fixed_size_list(DataType.float32(), 768),
        batch_size=8,                          # 上限，吃不到比上游 morsel 更大的批
    )
    def encode(self, images: daft.Series):
        return self.model.encode(images.to_pylist())


embedder = Embedder("/models/clip")            # 懒初始化，执行时才真正进 __init__

# 4. 读：默认 io_config 自动生效，不必每处再传一遍
df = daft.read_parquet("s3://bucket/meta/*.parquet").select("id", "url")
print(df.num_partitions())                     # 确认读侧真的切出了 task
df = df.into_partitions(2 * TOTAL_CPU)         # 并行度

# 5. 膨胀点：压批必须在 download / decode / 推理之前
df = (
    df.into_batches(16)
      .with_column("bytes", daft.col("url").url.download(max_connections=8))
      .with_column("image", daft.col("bytes").image.decode())
      .with_column("embedding", embedder.encode(daft.col("image")))
)
print(df.explain(show_all=True))               # 存下来，事后复盘要用

# 6. 写出：先 coalesce 收敛文件数，overwrite 保证重跑幂等
df.into_partitions(16).write_parquet(
    "s3://bucket/out/", write_mode="overwrite", write_success_file=True,
)
```

`set_planning_config(default_io_config=...)` 定了之后，所有 `read_*` / `write_*` / `url.download()` 不显式传 `io_config` 就都用它——只有个别路径要换账号或换 endpoint 时才在那一处单独传。改完记得在 driver 里把 effective `IOConfig` 打出来确认，但**不要**把密钥打进日志或 `explain()` 输出。完整参数见[读写参数](06-io-config.md)。

## 十 · 上线前勾一遍

- [ ] 作业以 `write_*` 收尾，路径上没有大结果 `collect` / `to_pandas` / `iter_rows` / `to_torch_*` / `to_ray_dataset` 等 driver 出口
- [ ] runner 是 Ray，提交方式是 RayJob 或 Jobs API（不是 `ray://` Client）
- [ ] head `num-cpus: "0"`；`requests == limits == num-cpus`
- [ ] 没有 `@daft.udf`；`default_morsel_size` 确实经 `set_execution_config` 传进去了
- [ ] 设了 `DAFT_MEMORY_LIMIT`，显式写了 `download(max_connections)`
- [ ] actor 资源 ≤ worker 资源，CPU 没被 actor 填满
- [ ] 重跑幂等（`overwrite` 或事务表），有输入 / 输出行数对账
- [ ] `partition_cols` 只有低基数列；写出前做了 coalesce
- [ ] 镜像是不可变 tag / digest，head 与 worker 同一镜像
- [ ] RayJob 设了 `activeDeadlineSeconds` / `shutdownAfterJobFinishes` / `ttlSecondsAfterFinished`
- [ ] `/tmp/ray` 有采集，保存了 `explain(show_all=True)` 和 effective config

这一页解决不了的，按 [首页](index.md) 的九页导航往下找。
