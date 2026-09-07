# 基本原理

先把四个概念分清。后面每一页调参、每一条禁令，都是这四个概念的推论。

## 1. DataFrame 默认惰性执行

`select`、`where`、`with_column`、`join`、`groupby` 只构建 LogicalPlan，不读数据、不占 worker。

下列操作会**触发执行**：

| 动作 | 会做什么 | 生产能不能当终点 |
|---|---|---|
| `show(n)` | 只跑出前 `n` 行所需工作 | 可以预览，不能当作业终点 |
| `collect()` | 执行完整计划并物化全部结果 | **大结果禁止** |
| `count_rows()` / `count()` | 跑完整计划只为得到一个数 | 中途当“进度检查”会重跑整图 |
| `to_pydict()` / `to_arrow()` / `to_pandas()` | 把结果拉到调用方进程 | **大结果禁止** |
| `to_torch()` / `to_ray_dataset()` | 物化后交给下游框架 | 确认体积后再用 |
| `write_parquet()` / `write_lance()` / `write_iceberg()` / `write_csv()` / `write_json()` | 执行并把数据落到外部存储 | **生产默认终点** |

不要为了“让前一步先跑”而在每个步骤后调用 `collect()`。这会切断优化机会、增加物化和内存压力。应尽量构建完整计划，最后统一写出。

建议在正式执行前保存计划：

```python
print(df.explain(show_all=True))
```

`show()` 是阻塞的，但只取预览所需结果，不等同于全量 `collect()`。巨大计划上把它当免费探活也不合适——它仍会启动执行。

## 2. Runner 决定执行范围

Runner 必须在应用入口设置一次，并且在创建或执行 DataFrame 前完成。

| Runner | 设置方式 | 适用场景 | 关键限制 |
|---|---|---|---|
| Native | `daft.set_runner_native()` | 单机开发、调试、中小数据 | 没有分布式 partition shuffle；`into_partitions` / `repartition` 基本是 no-op |
| Ray | `daft.set_runner_ray(...)` | 多核、多机、GPU、生产批处理 | 需要匹配的 Ray / Daft / Python 环境 |

```python
import daft

# 生产 RayJob 里 driver 已在集群内，连接本地 Ray 即可
daft.set_runner_ray()

# 交互开发才用 Client；生产长任务不要用
# daft.set_runner_ray("ray://head:10001")
```

也可以用环境变量 `DAFT_RUNNER=native` 或 `DAFT_RUNNER=ray`。`DAFT_RAY_ADDRESS` 已废弃，改用 `RAY_ADDRESS`。

Native 验证的是 Swordfish 的正确性与单机内存行为。它**不能**验证分布式调度、资源约束和故障语义。多节点生产必须用 Ray runner。

## 3. Partition 与 batch / morsel 不是同一件事

| | Partition | Batch / Morsel |
|---|---|---|
| 所在层 | Flotilla（跨 worker） | Swordfish（单 task 内） |
| 是什么 | 一个 Ray task 的工作单元 | 在算子之间流动的行批 |
| 影响 | task 数、并行度、shuffle、重算粒度、写出文件数 | 单次处理开销、峰值内存、UDF 单批大小 |
| 常用 API | `into_partitions(N)`、`repartition(N[, key])` | `into_batches(n)`、`default_morsel_size` |
| Native 上 | 基本 no-op | **有效**，单机也要靠它控内存 |

```python
# Ray 上拆分或合并为目标 partition 数；不按数据量均衡，不做全局 shuffle
df = df.into_partitions(64)

# Ray 上进行随机或按 key 的全局 shuffle
df = df.repartition(64)            # 随机
df = df.repartition(64, "user_id") # 按 key 哈希共址

# 在所有 runner 上控制后续算子的行批大小
df = df.into_batches(1_000)
```

`into_partitions` 便宜，只做机械拆分 / 合并。`repartition` 贵，走全局 shuffle。只想改 task 数时不要用 `repartition`。

## 4. 内存由多个资源池共同构成

一次 Daft 作业的峰值内存不只来自 DataFrame：

```text
进程内在途 morsel / batch
+ join / groupby / sort 等 blocking 算子的状态
+ UDF 模型常驻 RSS 和推理临时张量
+ 下载、解压、图像解码后的膨胀数据
+ Parquet row group 和并发 writer 缓冲
+ Ray Object Store（通常 mmap 到 /dev/shm，用量计入 Pod limit）
+ Python / Rust / Ray 系统进程
```

因此，单独增加 partition 或设置 `DAFT_MEMORY_LIMIT` 不能保证避免 OOM。OOM 时先判断是哪一项在涨，再决定动哪个旋钮。

容器里不设 `DAFT_MEMORY_LIMIT` 时，Daft 可能按**宿主机总内存**做预算。limit 8 GiB、宿主机 256 GiB，引擎会以为自己很宽裕，然后被 cgroup 直接 OOMKilled。

判断 Pod 是否触顶，只认 cgroup working set，不要用容器内 `psutil.virtual_memory()`。

## 决策速查

| 目标 | 用这个 |
|---|---|
| 调 task 数 / 文件数 / 并行度 | `into_partitions(N)` |
| 按 key 共址，或 join / groupby 前固定分布 | `repartition(N, key)` |
| 随机打散倾斜（昂贵） | `repartition(N)` |
| 控单 task 峰值内存（行批） | `into_batches(n)`，必要时再改 `default_morsel_size` |

起点公式（Ray 上）：

```text
Ray 总 CPU      = worker 副本数 × 每副本 num-cpus
初始 partitions = 2 × 总 CPU
搜索点          = 1× / 2× / 4× 总 CPU
```

行形态不同，morsel 起点完全不同。窄表可以上万行一批；解码后的图像或大 tensor 往往只要 1～16 行。详见 [Morsel 驱动的流式执行](morsel.md)。
