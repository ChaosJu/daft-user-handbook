# 资源怎么设

第 1 章已经推出结论：**worker pod 应当少而大**。一节点一个 Swordfish worker，I/O 与计算才能在进程内重叠。把 64 核拆成 64 个 1 核 pod，会把流水线切碎。

内存 limit ≠ 可用于计算的内存。`/dev/shm`、Ray、模型 RSS、在途 morsel 共享同一个额度。

## Worker：少而大

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

`/dev/shm` 是 tmpfs，**计入 cgroup 内存**。不要把它当成“额外的一块盘”再扣一次，也不要当成“不占 limit 的共享内存”。

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

Object Store 用 `rayStartParams.object-store-memory`（字节）显式设。不设则按可用内存比例自动取，容器里这个“可用”经常偏大。

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

CPU 用来做 decode / tokenize / 后处理。卡很闲、CPU 很高时，是预处理 batch 太大或 CPU 核不够，不是“该再叠一个模型”。

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

```text
Ray 总 CPU      = worker 副本 × num-cpus
初始 partitions = 2 × 总 CPU
搜索点          = 1× / 2× / 4×
```

```text
按 CPU 能放的 actor = floor( (worker CPU − 预留给 I/O 的 1～2 核) / 每 actor cpus )
按内存能放的 actor = floor( (limit − shm − 系统 − Daft heap) / 每 actor RSS )
最终 actor 数      = min(两者)，再留余量
```

单 actor 资源必须 ≤ 单 worker。违反第一条不等式，actor 会静默 `PENDING`。

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

破门禁时按 [根据监控调参](tune-from-metrics.md) 的表往下走：先减在途（morsel / download / actor），再考虑换更大的 worker 规格。不要用关 memory monitor 或漂 `latest` 镜像来“过线”。

## 和三条硬约束对齐的检查表

- [ ] head `num-cpus: "0"`
- [ ] 每个 group `requests == limits`
- [ ] `num-cpus == limits.cpu`，GPU 组还有 `num-gpus == limits.nvidia.com/gpu`
- [ ] `/dev/shm` sizeLimit ≥ `object-store-memory`，且计入 memory limit
- [ ] worker 少而大，固定规格关掉 autoscaling
- [ ] 设了 `DAFT_MEMORY_LIMIT`
- [ ] 镜像是不可变 tag / digest，head 与 worker 同一镜像
- [ ] 指标端口命名为 `metrics`，head 另有 `as-metrics` / `dash-metrics`
