# 生产禁区

这一页是上线门禁。每一条都来自真实故障：作业“成功”但数据少了、现场随 Pod 消失、或者调参把问题从软驱逐变成硬杀。

## 生产必须禁止

### 1. 对大结果 `collect()` / `to_pydict()` / `to_arrow()` / `to_pandas()`

这些调用把数据拉到 driver。driver 在 head 上，内存按调度规格配，不是按数据量配。生产终点是 `write_*`。

### 2. 中间 `collect()` 来“强制跑一步”

切断优化器，物化中间结果，毁掉谓词下推和列裁剪。需要分阶段就写出中间 sink，不要 `collect()`。

### 3. `iter_rows()` 或在 driver 上遍历全量结果

和 `collect()` 同类：把分布式结果变成单机循环。抽样用 `show()` / `limit()`，全量用写出。

### 4. 用 Ray Client（`ray://`）跑长生产作业

driver 在集群外，版本必须严格一致，长连接一断作业就死。生产用 RayJob 或 Jobs API，让 driver 待在 head 上。

### 5. 用 Native runner 跑多节点生产

Native 没有分布式 partition shuffle，也验证不了资源约束和故障语义。多机必须 `set_runner_ray()`。

### 6. head `num-cpus` 不为 0

UDF actor 会抢 GCS、Jobs API 和 Flotilla 的 CPU。head 固定 `num-cpus: "0"`。

### 7. `requests != limits`，或 `num-cpus != limits.cpu`

KubeRay 按 limits 上报，Ray 按 `num-cpus` 调度，cgroup 按 limit 硬限制。不一致就是逻辑超卖，表现为 throttling 或 Pending。

### 8. 继续使用 `@daft.udf`

0.7 废弃，0.8 移除。迁移到 `@daft.func` / `@daft.func.batch` / `@daft.cls`。

### 9. 重跑作业使用 `write_mode=append`

`append` 只加新 UUID 文件，不删旧文件，不幂等。重跑用 `overwrite` 或 `overwrite-partitions`。

### 10. 高基数 `partition_cols`

每个分区值一个 writer。`user_id` / `request_id` 当目录分区，内存和小文件一起爆。只分区 `dt`、`hour`、`region` 这类低基数列。

### 11. 关掉 Ray memory monitor 来“修复” OOM

`RAY_memory_monitor_refresh_ms=0` 只是把软驱逐换成 cgroup `OOMKilled`。先减 morsel / actor / 下载并发。

### 12. 以为设了 `DAFT_DEFAULT_MORSEL_SIZE` 环境变量就够了

Daft 不读这个变量，白名单里没有它。生效的唯一入口是 `set_execution_config(default_morsel_size=N)`。

它在很多部署里确实有效，是因为应用自己把它读出来再转进去了——所以判断一个部署里它有没有用，要看入口有没有那一行 `set_execution_config`，不是看 YAML。

### 13. 容器里不设 `DAFT_MEMORY_LIMIT`

默认按宿主机 RAM 做预算。limit 8 GiB、宿主机 256 GiB，引擎会继续放行直到被内核杀死。设成 `(limit − object store)` 的 70%～80%。

### 14. 不需要顺序时仍保持 `maintain_order=True`

默认 True，会引入排序缓冲。不需要行序就设 False。

### 15. 同一批节点上混跑常驻 RayCluster 和 RayJob

两套 CR 抢同一批资源，配额、指标和故障域都会乱。

### 16. 只靠 `kubectl logs` 排障

它只覆盖容器 stdout。Ray driver、UDF、raylet、GCS 的现场在 `/tmp/ray`，`kubectl logs` 看不见。

### 17. `/tmp/ray` 没有日志持久化

Pod 一重建或 RayJob TTL 一到，第 2～4 类日志全没。生产必须用 Fluent Bit sidecar（或等价采集）把 `/tmp/ray` 送走。

### 18. `image: latest`

digest 会漂。head / worker 必须用同一不可变 tag 或 digest。

### 19. 把 checkpoint 当成 exactly-once

Checkpoint 只减少 map-only 链路的重算。它不是作业续传，也不是 sink 的原子提交。外部 API 副作用不会因此变成 exactly-once。Lance sink 尤其要小心。

### 20. `on_error=ignore` 却没有 null 率门禁

作业会“成功”，失败行变成 null。必须对账 null 率、失败分类和输入 / 输出行数。

### 21. `download()` 留在默认 32

它会覆盖 `S3Config.max_connections=8`。按内存和对象大小显式下降。

### 22. 用 UDF actor 填满全部 CPU

actor 常驻，占的核拿不回来。I/O、写出、Ray 系统进程会饿死。至少留 1～2 核。

### 23. 单 actor 资源大于单 worker 资源

`cpus=4` 而 worker 只有 2 CPU → actor 永远 `PENDING`，作业静默卡死。先核对不等式。

### 24. `flight_shuffle_dirs` 放在 overlay `/tmp`

默认就是 `["/tmp"]`。容器 overlay 盘又慢又小。生产挂本地盘，并监控容量与清理。

### 25. RayJob 不设超时和 TTL

失败或卡住的集群会一直占节点。至少设 `activeDeadlineSeconds`、`shutdownAfterJobFinishes` 和 `ttlSecondsAfterFinished`。失败作业的 TTL 要比成功更长，以便排障。

### 26. 把 `show()` 当成巨大计划的免费探活

`show()` 会启动执行。预览可以，不要在全量生产计划上当心跳。

### 27. 流水线中途转成 pandas

`to_pandas()` 把分布式数据拉回单机，切断计划。聚合、join、过滤留在 Daft 里做。

### 28. 不对账输入 / 输出行数

写出成功不等于业务完整。记录输入行、输出行、失败行、唯一键、null 率。`_SUCCESS` 不能替代对账。

### 29. 在 Ray 上使用 `single_file` 写出

`single_file=True` 仅 Native。Ray 上会直接报错。集群写出靠目标文件大小和写出前 coalesce。

### 30. 不打印、不保存 `explain(show_all=True)`

partition 有没有生效、有没有意外 blocking sink、scan 有没有切开，只有计划能证明。每次跑都落盘。

### 31. 在海量数据上用中途 `count()` / `count_rows()` 当进度条

这会跑完整计划只为得到一个数。要进度看 Ray Dashboard / Daft 指标 / 阶段日志，不要重跑整图。

### 32. 未确认体积就 `to_torch()` / `to_ray_dataset()`

这些 API 会物化。训练对接前先 `limit` 或写出中间集，确认能装进目标进程。

## 生产允许的终点

这些是合法的生产 sink：

- `write_parquet`
- `write_lance`
- `write_iceberg`
- `write_csv`
- `write_json`
- 带 checkpoint 的 **map-only** 流水线（读 → filter / project / UDF → 上述 sink）

`show()` 只用于预览。`collect()` 只允许在结果确定能装进 driver 的冒烟或聚合后的小表上。

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
