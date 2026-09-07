# 生产禁区

上线门禁。每一条都来自真实故障：作业"成功"但数据少了、现场随 Pod 消失、或者调参把问题从软驱逐变成硬杀。

这一页只写**禁止什么**和**去哪看原因**，不重复机制。逐条勾完再上线。

## 一 · 数据出口


| #   | 禁止                                                              | 为什么                              |
| --- | --------------------------------------------------------------- | -------------------------------- |
| 1   | 对大结果 `collect()` / `to_pydict()` / `to_arrow()` / `to_pandas()` | driver 在 head 上，内存按调度规格配，不是按数据量配 |
| 2   | 中间 `collect()` 来"强制跑一步"                                         | 切断优化器，毁掉谓词下推和列裁剪。要分阶段就写中间 sink   |
| 3   | `iter_rows()` 或在 driver 上遍历全量结果                                 | 同上，把分布式结果变成单机循环                  |
| 4   | 流水线中途 `to_pandas()`                                             | 聚合、join、过滤留在 Daft 里做             |
| 5   | 把 `show()` 当成巨大计划的免费探活                                          | 它会启动执行                           |
| 6   | 用中途 `count()` / `count_rows()` 当进度条                             | 跑完整计划只为一个数。进度看 Dashboard 或阶段日志   |
| 7   | 未确认体积就 `to_torch()` / `to_ray_dataset()`                        | 这些 API 会物化，先 `limit` 或写中间集       |


合法的生产终点只有 `write_parquet` / `write_lance` / `write_iceberg` / `write_csv` / `write_json`，以及带 checkpoint 的 map-only 链路。`collect()` 只允许在结果确定装得进 driver 的冒烟或聚合后小表上。触发执行清单见[执行模型](02-execution-model.md)。

## 二 · 运行形态


| #   | 禁止                              | 为什么                                                                                          |
| --- | ------------------------------- | -------------------------------------------------------------------------------------------- |
| 8   | 用 Ray Client（`ray://`）跑长生产作业    | driver 在集群外，版本要求严格，长连接一断作业就死。用 RayJob，见[部署](03-deploy-kuberay.md)                            |
| 9   | 用 Native runner 跑多节点生产          | 没有分布式 shuffle，验证不了资源约束和故障语义                                                                  |
| 10  | 同一批节点混跑常驻 RayCluster 和 RayJob   | 两套 CR 抢资源，配额、指标、故障域全乱                                                                        |
| 11  | `image: latest`                 | digest 会漂。head / worker 必须同一不可变 tag 或 digest                                                 |
| 12  | RayJob 不设超时和 TTL                | 卡住的集群会一直占节点。至少设 `activeDeadlineSeconds`、`shutdownAfterJobFinishes`、`ttlSecondsAfterFinished` |
| 13  | 继续使用 `@daft.udf`                | 0.7 废弃，0.8 移除。迁移到 `@daft.func` / `.batch` / `@daft.cls`，见 [UDF](05-udf.md)                   |
| 14  | 在 Ray 上使用 `single_file=True` 写出 | 仅 Native 支持，Ray 上直接报错                                                                        |




## 三 · 资源与内存


| #   | 禁止                                              | 为什么                                                    |
| --- | ----------------------------------------------- | ------------------------------------------------------ |
| 15  | head `num-cpus` 不为 0                            | UDF actor 会抢 GCS / Jobs API / Flotilla 的 CPU           |
| 16  | `requests != limits`，或 `num-cpus != limits.cpu` | 逻辑超卖，表现为 throttling 或 Pending                          |
| 17  | 容器里不设 `DAFT_MEMORY_LIMIT`                       | 默认按宿主机 RAM 做预算，会一路放行到被内核杀死                             |
| 18  | 关掉 Ray memory monitor 来"修复" OOM                 | `RAY_memory_monitor_refresh_ms=0` 只是把软驱逐换成 `OOMKilled` |
| 19  | 用 UDF actor 填满全部 CPU                            | actor 常驻，占的核拿不回来。至少留 1～2 核                             |
| 20  | 单 actor 资源大于单 worker 资源                         | actor 永远 `PENDING`，作业静默卡死                              |
| 21  | `flight_shuffle_dirs` 放在 overlay `/tmp`         | 默认就是 `["/tmp"]`，容器 overlay 盘又慢又小                       |


三条部署硬约束的完整推导见[架构](01-architecture.md)，规格与预算公式见[资源与调参](08-tuning-runbook.md)。

## 四 · 调参


| #   | 禁止                                      | 为什么                                                                  |
| --- | --------------------------------------- | -------------------------------------------------------------------- |
| 22  | 以为设了 `DAFT_DEFAULT_MORSEL_SIZE` 环境变量就够了 | Daft 不读它。唯一入口是 `set_execution_config`，见[执行模型](02-execution-model.md) |
| 23  | 把 `into_batches` 插在膨胀算子**之后**           | 行批要求向上游传播，插在 decode 之后对 decode 无效                                    |
| 24  | `download()` 留在默认 32                    | 它会覆盖 `S3Config.max_connections=8`，见[读写参数](06-io-config.md)           |
| 25  | 不需要顺序时仍保持 `maintain_order=True`         | 默认 True，会引入排序缓冲                                                      |
| 26  | 不打印、不保存 `explain(show_all=True)`        | partition 有没有生效、有没有意外 blocking sink，只有计划能证明                          |




## 五 · 正确性与幂等


| #   | 禁止                             | 为什么                                                  |
| --- | ------------------------------ | ---------------------------------------------------- |
| 27  | 重跑作业使用 `write_mode=append`     | 只加新 UUID 文件，不删旧文件，不幂等。用 `overwrite`                  |
| 28  | 高基数 `partition_cols`           | 每个分区值一个 writer，内存和小文件一起爆。只放 `dt` / `hour` / `region` |
| 29  | `on_error=ignore` 却没有 null 率门禁 | 作业会"成功"，失败行变成 null                                   |
| 30  | 不对账输入 / 输出行数                   | `_SUCCESS` 不能替代对账。记录输入、输出、失败、唯一键、null 率              |
| 31  | 把 checkpoint 当成 exactly-once   | 它只减少 map-only 链路的重算，不是作业续传，也不是 sink 的原子提交            |




## 六 · 可观测


| #   | 禁止                   | 为什么                                                                                                 |
| --- | -------------------- | --------------------------------------------------------------------------------------------------- |
| 32  | 只靠 `kubectl logs` 排障 | 它只覆盖容器 stdout，Ray 现场在 `/tmp/ray`                                                                    |
| 33  | `/tmp/ray` 没有日志持久化   | Pod 一重建或 TTL 一到，driver / UDF / raylet / GCS 日志全没。用 Fluent Bit sidecar，见[日志与监控](07-observability.md) |




## 上线前再勾一遍

- [ ] 作业以 `write_*` 收尾，路径上没有大结果 `collect` / `to_pandas`
- [ ] runner 是 Ray，提交方式是 RayJob 或 Jobs API
- [ ] head `num-cpus=0`，`requests == limits == num-cpus`
- [ ] 没有 `@daft.udf`；`default_morsel_size` 确实经 `set_execution_config` 传进去了
- [ ] 设了 `DAFT_MEMORY_LIMIT`，设了 `download(max_connections)`
- [ ] 重跑幂等（overwrite / 事务表），有行数对账
- [ ] `/tmp/ray` 有采集，RayJob 有超时和 TTL
- [ ] 保存了 `explain(show_all=True)` 和 effective config
- [ ] actor 资源 ≤ worker，CPU 没被 actor 填满