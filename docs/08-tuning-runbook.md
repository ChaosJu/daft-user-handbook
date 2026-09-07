# 资源与调参

先确认资源真的到位。`ray status` 里 Total CPU 对不上时，下面所有"调参"都是在解错的题。

## 三十秒体检

作业还在跑时，这三条足够判断要不要继续等。

```bash
# 1 资源是否真的都到位：Used / Total 应接近满配
kubectl -n "$NS" exec -c ray-head "$HEAD" -- ray status

# 2 有没有 Pod 在反复重启
kubectl -n "$NS" get pods -l ray.io/cluster="$CLUSTER"

# 3 actor 是否稳定
kubectl -n "$NS" exec -c ray-head "$HEAD" -- \
  ray list actors --address http://127.0.0.1:8265
```

```text
Total CPU 少于预期   有 worker 没起来，先查 Pending，不要急着调参
RESTARTS 在涨        内存问题，现在就该保留现场
actor RESTARTING     初始化在反复重做，吞吐会持续恶化，等下去没有意义
```

对照 `ray.cluster_resources()`：可见 CPU 必须等于 `worker 副本数 × num-cpus`。差一个 worker，就先 `kubectl describe pod` 看调度 / 镜像 / 资源，不要加 partition。

## 症状 → 先看 → 动作

| 现象 | 先看 | 可能原因 | 动作 |
|---|---|---|---|
| CPU 低 + PENDING 多 | pending reason、逻辑 CPU | partition 太少，或 actor 资源不匹配 | 加 partition；核对 actor `cpus` 与 worker `num-cpus` |
| CPU 低 + `PENDING_ARGS_AVAIL` | pull manager、ObjectRef 位置 | 跨节点传输或上游未完成 | 减小 partition 矩阵，查上游长尾 |
| CPU 低，task 很少 | `df.num_partitions()`、scan 布局 | 输入文件少 / 开关没开，切不出 task | `into_partitions`；打开 scan split/merge |
| CPU 被 throttling | cAdvisor throttling、pod spec | `num-cpus` ≠ CPU limit | 统一两者，别超卖 |
| working set 爬升 / 逼近 limit | working set/limit、RSS、Object Store | morsel / 并发过大、blocking 算子、泄漏 | 先减 morsel / `into_batches`，再减 actor 并发和 `download` 连接 |
| 内核 `OOMKilled` | `last_terminated_reason` | cgroup 触顶 | 同上，并核对 `DAFT_MEMORY_LIMIT` 与内存公式 |
| Ray eviction 上升 | `ray_memory_manager_worker_eviction_total` | Ray 软杀 | **不要关 memory monitor**；减在途数据 |
| Object Store spill | `ray_object_store_memory{Location=SPILLED}` | store 太小或 partition / shuffle 太碎 | 加大 `/dev/shm` 或减 refs；大 shuffle 评估 Flight |
| 很多 task 但更慢 | task 数、文件数、GCS 延迟 | partition 太碎、小文件、shuffle 矩阵过大 | 减 partition、合并 scan task、compaction |
| 输出大量小文件 | 写出前 partition、`partition_cols` 基数 | 计算并行度直接变成文件数 | 写出前 coalesce；降目录分区基数 |
| actor `RESTARTING` | `ray list actors` | 被杀后重做初始化（通常是内存） | **先解决内存，不是加并发** |
| actor 大量 PENDING | 资源需求、ready timeout | 单 actor 大于 worker，或总量超集群 | 降 actor 资源 / 数量；timeout 只是缓解 |
| 长尾明显 | task duration 分布、文件大小 | 数据倾斜、文件不均 | 加 partition、拆 source、查 key skew |
| Lance task 远大于 CPU | fragment 数、`num_small_files` | fragment 过碎 | 加 `fragment_group_size`，安排 compaction |
| Head 内存 / 延迟上升 | GCS、ObjectRef 数 | task 或 shuffle 元数据爆炸 | 减 partitions；必要时 Flight |
| 单 task 像在跑全量 | `ray_tasks`、physical plan | `into_partitions` 没生效或 runner 不是 Ray | 保存 `explain(show_all=True)`，确认调用顺序 |

三个内存口径走势不同，问题在不同池：

```text
只有 cgroup 涨          进程 / 临时对象 / 模型 RSS / 在途 morsel
只有 Object Store 涨    Ray 对象生命周期、shuffle
只有 Ray 内部账涨       metadata / lineage
```

## 调参顺序与纪律

固定代码、镜像、数据快照、worker 规格。每轮只改一个主变量。

```text
0  Total CPU 对得上，没有 Pending / CrashLoop
1  保存 explain(show_all=True)，确认 blocking sink 与分区
2  sweep partition（1× / 2× / 4× 总 CPU）
3  sweep morsel 或膨胀点前的 into_batches
4  sweep UDF actor / batch_size / max_concurrency
5  sweep download / S3 max_connections
6  有 join / groupby / sort 再比 shuffle
7  最后改写出 partition、文件大小、row group、压缩
```

六条纪律：

1. **一轮只改一个主变量。** 两个变量一起动、结果变好了，你不知道该保留哪个。
2. **代码、镜像、数据快照、worker 规格保持不变。** 拐点必须能归因到某一层。
3. **先看正确性 / OOM / retry，再比吞吐。** 错的结果不参与排名。
4. **每个配置至少跑 2 次。** 波动超过 10% 先查环境噪声和缓存。
5. **不要关掉 Ray memory monitor 来"解决" OOM。** 那只是把软驱逐换成 cgroup `OOMKilled`。
6. **不要只调大 `num-cpus` 而不改 CPU limit。** 那是制造逻辑超卖。

纯 map-only 链路（读 → UDF → 写）没有 shuffle，不要提前碰 shuffle 算法。

## 六个不配就出事的默认值

| 参数 | 不设的后果 |
|---|---|
| `default_io_config` | 对象存储并发、重试、超时全是默认值 |
| `DAFT_MEMORY_LIMIT` | 按宿主机总内存做预算，容器里等于没配额 |
| `DAFT_MAX_ASYNC_UDF_INFLIGHT_TASKS` | 默认 64，异步 UDF 在途量的隐式天花板 |
| `download(max_connections)` | 默认 32，且会顶掉 `S3Config` |
| `OTEL_EXPORTER_OTLP_*` | Daft 指标出不到 Prometheus |
| `DAFT_ANALYTICS_ENABLED` | 默认开启，内网 / 离线应设 `0` |

另外记住：Daft 不读 `DAFT_DEFAULT_MORSEL_SIZE`，`default_morsel_size` 必须经 `set_execution_config` 传进去。机制见[执行模型](02-execution-model.md)。

## Morsel 怎么调

按行形态选起点（行）：

| 行形态 | 起点 | 典型列 |
|---|---:|---|
| 窄表、标量列 | 1024 ～ 131072 | int / float / 短字符串 |
| 大字符串、百 KB 级对象 | 16 ～ 64 | 长文本、小图片 bytes、JSON blob |
| MB 级对象、解码结果 | 4 ～ 16 | 解码图像、解压后文档 |
| 大 tensor / 重中间状态 | 1 ～ 8 | 视觉模型输入、大 embedding |

起点不是最优值。用 1× / 2× / 4× 做小范围 sweep，同时看 working set 与吞吐。

**减小** morsel / `into_batches`：OOM 或 working set 逼近 limit、Object Store spill 上升、decode / explode / UDF 后单行暴涨、actor 反复 `RESTARTING`。

**增大**：CPU 低但内存安全（P95 working set < 60% limit）、调度开销高 / 极碎小批占满时间线、窄表标量链路模型吞吐吃不满。

**调小无效时**：pipeline 里有 blocking sink（见[架构](01-architecture.md)）。先改算法、减 key 基数、换 broadcast / 去 shuffle，再回头看 morsel。不要从 131072 直接砍到 1，除非行已经是 MB 级。

操作顺序：

```text
1  explain()，确认膨胀点前后没有意外的 blocking sink
2  只在膨胀点之前插 into_batches
3  显式写 download(max_connections=...)
4  仍 OOM 再降全局 default_morsel_size
5  内存安全但 CPU 低，再把批往上加找拐点
```

## Worker：少而大

[架构](01-architecture.md)已经推出结论：一节点一个 Swordfish worker，I/O 与计算才能在进程内重叠。

| 做法 | 结果 |
|---|---|
| 8 个 worker × 8 CPU / 48Gi | 每个节点内部流水线完整，object store 本地命中率高 |
| 64 个 worker × 1 CPU / 6Gi | 每核一个进程，下载完才能算，跨节点 pull 增多 |

GPU 同理：按卡数开 replicas，每卡配够 CPU 和内存，不要在一张卡上叠多个互不相让的完整模型 actor（除非模型明确支持分数 GPU 且测过）。

固定规格生产把 `replicas` / `minReplicas` / `maxReplicas` 写成一样，关掉 autoscaling。弹性另做一轮验证，不要和性能基线混在一起。

## 内存预算公式

```text
Pod memory limit
  = Ray 系统进程（raylet / GCS 代理 / core worker）
  + Object Store（/dev/shm，实际用量计入 limit）
  + Daft Python / Rust heap
  + 模型 RSS × 每 worker actor 数
  + 在途 morsel（并发 task × 批行数 × 单行体积）
  + 20%～30% 余量
```

`/dev/shm` 是 tmpfs，**计入 cgroup 内存**。不要把它当成"额外的一块盘"再扣一次，也不要当成"不占 limit 的共享内存"。

```text
48Gi limit
  − 8Gi  /dev/shm（object-store-memory 约 4～8Gi，shm 要大于它）
  − 4Gi  Ray 系统
  − 2Gi  Daft heap
  − N × 模型 RSS
  − 在途 morsel
  − 20%～30% 余量
  = 真正能给 actor 和在途数据的预算
```

Object Store 用 `rayStartParams.object-store-memory`（字节）显式设。不设则按可用内存比例自动取，容器里这个"可用"经常偏大。

## `DAFT_MEMORY_LIMIT`

这是 Swordfish 内部 MemoryManager 的软配额，单位字节，**纯环境变量**，不在 `set_execution_config` 里。

- 它只管声明了内存预算的算子，不管模型 RSS、object store、cgroup。
- 不设时默认按系统总内存——容器里经常读到宿主机 RAM。
- 建议：`(Pod limit − object store)` 的 **70%～80%**。

```bash
# 例：worker 48Gi，object store 8Gi → 可用约 40Gi，Daft 池取 28～32Gi
export DAFT_MEMORY_LIMIT=30064771072   # 28Gi
```

它替代不了把 morsel / actor / 下载并发配对。它只是防止引擎按宿主机口径继续放行。

## 起步配方

### CPU worker

```text
8 CPU / 48Gi
/dev/shm 8Gi
object-store-memory ≈ 4Gi（小于 shm）
num-cpus: "8"
requests == limits
DAFT_MEMORY_LIMIT ≈ 28Gi
初始 partition = 2 × (副本数 × 8)
```

适合 map-only 多模态 / 特征流水线的第一轮基线。行很胖或模型很大时，先加内存，不要先加副本。

### GPU worker

```text
8 CPU / 64Gi + 1 GPU
/dev/shm 8Gi
num-cpus: "8"
num-gpus: "1"
requests / limits 都写 nvidia.com/gpu: 1
actor：每卡 1 个完整模型，除非测过分数 GPU
```

CPU 用来做 decode / tokenize / 后处理。卡很闲、CPU 很高时，是预处理 batch 太大或 CPU 核不够，不是"该再叠一个模型"。

### Head

```text
2 CPU / 8Gi
num-cpus: "0"
/dev/shm 2Gi
entrypointNumCpus: 0
```

head 跑 GCS、Dashboard、Jobs API、Flotilla 和 driver。不要在 head 上跑 UDF actor。metadata 压力（超多 partition / 巨大 shuffle 矩阵）先减 partition，而不是无限给 head 加内存。

多作业共享的常驻集群可以把 head 提到 4 CPU / 16Gi，但 `num-cpus` 仍然是 `"0"`。

## Partition 与 actor 怎么跟资源走

partition 起点公式见 [Partition](04-partition.md)。actor 数由两条不等式和内存共同决定，完整推导见 [UDF](05-udf.md)：

```text
最终 actor 数 = min(按 CPU 能放的, 按内存能放的)，再留 1～2 核给 I/O 和 Ray
```

单 actor 资源必须 ≤ 单 worker。违反这条，actor 会静默 `PENDING`。

## 门禁

判断触顶只认 cgroup working set，不认容器内 `psutil`。

```text
P95 working set   ≤ Pod limit × 80%
Peak working set  ≤ Pod limit × 90%
稳定负载增长斜率   < 500 MB / hour
```

同时对照：

- 没有 `OOMKilled`，`RESTARTS` 不涨
- `ray_memory_manager_worker_eviction_total` 接近 0
- Object Store `SPILLED` 只在可解释的尖峰出现
- CPU throttling 接近 0
- `ray status` 的 Total CPU 等于设计值

破门禁时按上面的症状表往下走：先减在途（morsel / download / actor），再考虑换更大的 worker 规格。不要用关 memory monitor 或漂 `latest` 镜像来"过线"。

## 上线规格检查表

- [ ] head `num-cpus: "0"`
- [ ] 每个 group `requests == limits`
- [ ] `num-cpus == limits.cpu`，GPU 组还有 `num-gpus == limits.nvidia.com/gpu`
- [ ] `/dev/shm` sizeLimit ≥ `object-store-memory`，且计入 memory limit
- [ ] worker 少而大，固定规格关掉 autoscaling
- [ ] 设了 `DAFT_MEMORY_LIMIT`
- [ ] 镜像是不可变 tag / digest，head 与 worker 同一镜像
- [ ] 指标端口命名为 `metrics`，head 另有 `as-metrics` / `dash-metrics`

## 每轮必须留下的证据

```text
run_id、Daft / Ray / Python 版本、代码 commit、镜像 digest
worker 规格、partition、morsel、UDF 并发、effective config
explain 输出、输入 / 输出行数、null 率
wall time、CPU、working set / limit、Object Store、spill
task P50 / P95、retry、失败分类
```

没有计划、没有行数账本，事后只能复现，不能复盘。

## 作业自己要写出来的产物

平台给指标和日志。作业还必须自己落：

| 产物 | 回答什么 |
|---|---|
| run 元数据（镜像、版本、全部生效配置、输入快照） | 这次到底跑的是什么 |
| `explain(show_all=True)` | 算子下推、分区是否真的生效 |
| 阶段计时 | 慢在哪一段 |
| 行数账本（输入 / 输出 / 失败） | 是不是悄悄丢了数据 |
| 按原因分桶的错误计数 | 失败集中在哪个依赖 |
| `finally` 里的终态标记 | 区分"失败"和"根本没写出来" |

错误计数不要只写 `failed: 1273`，要能指向责任方：`input_read_failed` / `udf_exec_failed` / `sink_write_failed`。
