# 最佳实践总览

第 1、2 章讲系统怎么工作。这一章讲生产怎么配。每一页对应一个独立旋钮或一份必须遵守的清单。

## 实践章节

| 页 | 解决什么 |
|---|---|
| [KubeRay 部署](deploy-kuberay.md) | driver 放哪、集群谁建谁删、CRD 怎么选 |
| [RayJob 实战](rayjob-hands-on.md) | 按官方 Quickstart apply 我们的 YAML |
| [Partition](partition.md) | task 数、scan 切分、计算分区 ≠ 写出分区 |
| [Batch 与 Morsel](batch.md) | 膨胀算子前压批、`download` 默认 32 会顶掉 S3 配置 |
| [UDF](udf.md) | `@daft.func` / `batch` / `cls`，actor 两个不等式 |
| [读写参数配置](io-config.md) | Parquet / Lance / S3 / `write_mode` |
| [生产禁区](production-donts.md) | 上线前必须过一遍的反模式清单 |

可 apply 的清单在 [`examples/kuberay/`](../../examples/kuberay/)。生产默认是 `20-rayjob.yaml`（临时集群）；连续调参用 `40-raycluster.yaml` + `41-rayjob-existing.yaml`（常驻集群），两者不要同时 apply。

## 调参纪律

1. **一轮只改一个主变量。** 两个变量一起动、结果变好了，你不知道该保留哪个。
2. **代码、镜像、数据快照、worker 规格保持不变。** 拐点必须能归因到某一层。
3. **先看正确性 / OOM / retry，再比吞吐。** 错的结果不参与排名。
4. **每个配置至少跑 2 次。** 波动超过 10% 先查环境噪声和缓存。
5. **不要关掉 Ray memory monitor 来“解决” OOM。** 那只是把软驱逐换成 cgroup `OOMKilled`。
6. **不要只调大 `num-cpus` 而不改 CPU limit。** 那是制造逻辑超卖。

## 建议顺序

```text
资源到位（Total CPU 对得上）
  → partition（1× / 2× / 4× 总 CPU）
  → morsel / into_batches
  → UDF actor 与 batch_size
  → I/O 并发
  → 写出布局
```

出现 join / groupby / sort 才进入 shuffle 调参。纯 map-only 链路（读 → UDF → 写）不要提前碰 shuffle 算法。

## 六个不配就出事的默认值

| 参数 | 不设的后果 |
|---|---|
| `default_io_config` | 对象存储并发、重试、超时全是默认值 |
| `DAFT_MEMORY_LIMIT` | 按宿主机总内存做预算，容器里等于没配额 |
| `DAFT_MAX_ASYNC_UDF_INFLIGHT_TASKS` | 默认 64，异步 UDF 在途量的隐式天花板 |
| `download(max_connections)` | 默认 32，且会顶掉 `S3Config` |
| `OTEL_EXPORTER_OTLP_*` | Daft 指标出不到 Prometheus |
| `DAFT_ANALYTICS_ENABLED` | 默认开启，内网 / 离线应设 `0` |

另外记住：Daft 不读 `DAFT_DEFAULT_MORSEL_SIZE`，`default_morsel_size` 必须经 `set_execution_config` 传进去（应用入口自己读环境变量再转是可以的）。

上线前把 [生产禁区](production-donts.md) 整页勾完。
