# UDF

0.7 之后只用一套装饰器。`@daft.udf` 自 0.7.0 废弃，**0.8.0 移除**。现存代码里的 `num_cpus` / `num_gpus` / `concurrency` 对应新 API 的 `cpus` / `gpus` / `max_concurrency`。

## 该用哪一个

```python
@daft.func                # 逐行标量函数
@daft.func.batch(...)     # 批处理，函数拿到 Series
@daft.cls(...)            # 有状态（模型常驻），方法上配 @daft.method
```

| 装饰器 | 输入 / 输出 | 必须 `return_dtype` | 典型用途 |
|---|---|---|---|
| `@daft.func` | 1 行 → 1 行 | 否（可用 type hint） | 轻量 Python 变换 |
| `@daft.func` + `async def` | 1 行 → 1 行（协程） | 否 | I/O bound 外部 API |
| `@daft.func` + generator | 1 行 → N 行 | Iterator 类型 hint | explode 式展开 |
| `@daft.func.batch` | `Series` → `Series` / list / numpy / arrow | **是** | 向量化、无状态批推理 |
| `@daft.cls` + `@daft.method` | 绑定 class 实例 | 同 func | 模型常驻、actor 池 |
| `@daft.method.batch` | batch method | **是** | class 内批推理 |

参数中没有 `Expression` 时会立刻当普通 Python 函数执行；含 `Expression` 才进入计划。优先用 Daft 原生表达式，UDF 有序列化、进程切换和资源管理成本。

```python
import daft
from daft import DataType

@daft.func
def normalize(text: str) -> str:
    return text.strip().lower()

@daft.func.batch(return_dtype=DataType.float32(), batch_size=32)
def score_batch(texts: daft.Series) -> list[float]:
    return model.predict(texts.to_pylist())

@daft.cls(gpus=1, max_concurrency=2)
class Embedder:
    def __init__(self):
        self.model = load_model()

    @daft.method.batch(return_dtype=DataType.fixed_size_list(DataType.float32(), 768))
    def encode(self, texts: daft.Series):
        return self.model.encode(texts.to_pylist())
```

同步 `@daft.func` **不能**设 `max_concurrency`，会直接 `ValueError`。要多实例必须用 `@daft.cls`。

## 参数

| 参数 | 作用 | 要点 |
|---|---|---|
| `cpus` / `gpus` | 每实例资源需求 | 只影响放置：8 CPU 机器 + `cpus=4` → 最多 2 个实例 |
| `max_concurrency` | **同步 = actor 进程数；async = 协程并发数** | 同名两种语义，最容易配错 |
| `batch_size` | 单批最大行数，仅 batch API | 上限不是保证值，上游 morsel 更小就填不满（见[执行模型](02-execution-model.md)） |
| `use_process` | 每实例独立进程 | 不设时引擎自选；绕 GIL、隔离 native 崩溃 |
| `max_retries` | 单次调用（一行或一批）重试次数 | 默认 0；带指数退避。**不是** actor 重启 |
| `on_error` | `raise` / `log` / `ignore` | `log` / `ignore` 把结果**置 null** |
| `ray_options` | 透传 Ray executor | 禁止再传 `num_cpus` / `num_gpus` / `memory` |
| `return_dtype` / `unnest` | 输出类型 / 展开 struct | batch 必须显式 `return_dtype` |
| `actor_udf_ready_timeout` | actor 就绪超时 | `set_execution_config`，**默认 120s** |

`gpus` 支持 0～1 的小数；大于 1 必须是整数。一张卡同时只服务一个完整 GPU actor。

`actor_udf_ready_timeout` 默认 120 秒。模型冷启动慢就要调大，否则 actor 还没就绪作业已判失败——RayJob 每次冷启动尤其容易撞上。

## `max_concurrency` 的两种语义

```text
同步 Class UDF   daft.cls(cpus=C, max_concurrency=N)
  → 全集群最多 N 个并行 UDF 实例 = N 个常驻 actor
  → 占用 CPU = N × C，可以直接乘出来

异步 Class / async func   max_concurrency=N
  → N 是每个 class worker 内的并发协程数
  → 不是 “N 个各占 c CPU 的 actor”
  → 实际 CPU 预留取决于 Daft 建了几个 class worker
```

异步侧的真实并发不能从配置数字反推。以 `ray status`、Dashboard 的 Actors 页面和实际占用的 CPU 为准。

异步 UDF 还有一条硬上限：

```text
DAFT_MAX_ASYNC_UDF_INFLIGHT_TASKS  默认 64
真实在途量 = max_concurrency × class worker 数，再被 64 截断
```

需要更高在途时显式加大这个环境变量，并同步看内存。

## 两个必须同时满足的不等式

```text
① 单 actor 资源  ≤  单 worker 能提供的资源
   cpus=4 而 worker 只有 2 CPU  →  actor 永远 PENDING，作业静默卡死

② Σactor + 普通 task + 系统进程  ≤  集群总量
   actor 是常驻的，它占的 CPU 在整个作业期间都拿不回来
   必须给 I/O、写出、Ray 系统进程留余量，不能把 CPU 排满
```

actor 资源大于 worker 资源时，Ray 不会报一个响亮的“配错了”，只会一直 `PENDING`。[三十秒体检](08-tuning-runbook.md)里 `ray list actors` 就是为这个准备的。

## 每个 worker 能放几个 actor，由内存决定

```text
单 worker 可用内存 = Pod memory limit − object store(/dev/shm) − 系统开销
每 actor 常驻内存  = 模型 / 状态 RSS + 单批在途数据

可放的 actor 数 = floor(单 worker 可用内存 ÷ 每 actor 常驻内存)
```

CPU 算出来能放 4 个、内存只够放 2 个 → **以内存为准**。按 CPU 排满的结果就是 worker 被 OOMKilled，actor 反复重启到耗尽次数，作业挂。

```text
actor 数 = min(按 CPU 能放的, 按内存能放的)
然后至少再留 1～2 核给 I/O 和 Ray
```

## UDF 的 `batch_size` 与上游 morsel

`batch_size=N` 是单批上限，不是保证值。UDF 吃到的批不会大于上游 morsel：

```text
上游 into_batches(16) + UDF batch_size=128  → 实际最多 16 行
上游 morsel=8192 + UDF batch_size=8         → UDF 再切细，合理
```

先定膨胀点之后的行批，再让 UDF `batch_size` ≤ 这个值。与 `enable_dynamic_batching` 同时打开会冲突。

## 重试与 `on_error`

`max_retries` 重试的是**一次 UDF 调用**，不是 actor。

```text
默认 0
重试单位  同步 row-wise = 这一行；异步 / batch = 整批
退避      100ms 起，×2，上限 60s，±25% 抖动
耗尽后    才轮到 on_error
```

actor 被 OOM 杀掉之后由 Ray 负责：重启 actor 最多 4 次，重试那一批最多 4 次。这和 `max_retries` 是两条路。

`on_error`：

| 值 | 行为 | 生产 |
|---|---|---|
| `raise`（默认） | 作业失败 | 默认选这个 |
| `log` | 打日志，该行 / 批变 null | 必须有 null 率门禁 |
| `ignore` | 静默变 null | 同上，且更危险 |

`ignore` 而不做 null 率门禁，等于允许作业“成功”但少了几行。批 UDF 失败时影响范围可能是整批，不只是单行。外部调用必须幂等，否则 `max_retries` 会制造重复副作用。

## 生产检查

- 不要继续用 `@daft.udf`。
- 不要把所有 CPU 填满 UDF actor。
- 不要让单 actor 的 `cpus` / `gpus` / 内存大于单 worker。
- RayJob 冷启动把 `actor_udf_ready_timeout` 调到模型真实加载时间之上。
- 打印 actor 数、单实例 RSS、`on_error` 策略和 null 率。
