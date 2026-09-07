# 根据监控调参

先确认资源真的到位。`ray status` 里 Total CPU 对不上时，下面所有“调参”都是在解错的题。

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

## 调参顺序

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

先看正确性、OOM、retry，再比吞吐。每个配置至少跑 2 次；波动 > 10% 先查环境噪声。

不要：

- 关掉 Ray memory monitor
- 只加大 `num-cpus` 不改 CPU limit
- 两个旋钮一起动
- 在 Total CPU 错误时继续 sweep

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
| `finally` 里的终态标记 | 区分“失败”和“根本没写出来” |

错误计数不要只写 `failed: 1273`，要能指向责任方：`input_read_failed` / `udf_exec_failed` / `sink_write_failed`。
