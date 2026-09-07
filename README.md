# Daft 用户指导手册

面向生产使用的 **Daft on Ray** 手册：把架构、执行原理、部署实践和运维调参收成一份可执行的用户指南。

本手册不是 API 百科，而是从现有内部文档提炼出的生产共识。官方 API 以 [docs.daft.ai](https://docs.daft.ai) 为准。

## 四根支柱

| 章 | 主题 | 要回答的问题 |
|---|---|---|
| [1. 架构介绍](docs/01-architecture/index.md) | Daft on Ray | Flotilla 与 Swordfish 怎么分工？为什么 worker 要少而大？ |
| [2. 基本原理](docs/02-principles/index.md) | Lazy、Runner、Partition、Morsel | 什么会触发执行？partition 和 batch 为什么不是一回事？ |
| [3. 最佳实践](docs/03-best-practices/index.md) | KubeRay、分区、批、UDF、I/O、禁区 | 生产该怎么部署、怎么配、绝对不能做什么？ |
| [4. 日志、监控与调参](docs/04-observability/index.md) | 观测栈与资源 | 出了问题看哪一层？数字怎么转成下一轮参数？ |

## 本地预览

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux / macOS
# source .venv/bin/activate

pip install -r requirements.txt
mkdocs serve
```

浏览器打开 <http://127.0.0.1:8000>。构建静态站点：

```bash
mkdocs build
```

产物在 `site/`。

## 资料来源

本手册蒸馏自以下内部文档（以仓库 `main` 为准，升级 Daft 后请重新核对实验性参数）：

- `Daft/docs/daft-on-ray-handbook.md` —— 架构、KubeRay 部署、参数分层、观测与可靠性
- `Daft/docs/daft-user-guide-cn.md` —— Python API、I/O、UDF、分区与批次
- `Daft/docs/daft-on-ray-performance-runbook.md` —— 调参顺序、事中判读、资源模板

公开 API 与最新签名以官方文档为准：<https://docs.daft.ai>

## 示例清单

三套清单，都是 RayJob 形态，区别在"跑什么、用什么镜像"：

| | [`examples/quickstart/`](examples/quickstart/) | [`examples/docker/`](examples/docker/) | [`examples/kuberay/`](examples/kuberay/) |
|---|---|---|---|
| 镜像 | `rayproject/ray:2.46.0-py312-cpu`（官方） | **`daft-ray-ops`**（自建，通用） | `daft-audio:offline`（自建，ASR 专用） |
| Daft 来源 | 运行时 `uv` + `pip` 装 | 镜像里 | 镜像里 |
| 业务 | 官方文档的示例脚本 | 同上（baked 版） | 真实音频 ASR 流水线 |
| 规模 | 1 worker × 2C/4Gi | 可配置 | 10 worker × 2C/10Gi |
| 外部依赖 | 无 | 无 | MinIO + Mock LLM |
| 离线可用 | 否 | 是 | 是 |
| 用途 | 零构建验证链路通不通 | **通用生产镜像模板** | 完整 benchmark 参考 |

先跑 `quickstart` 确认 KubeRay 装对了，再看 `kuberay`。两者互为对照——官方 quickstart 同时踩了手册里三条生产红线（`collect()`、运行时装依赖、依赖不锁版本），对着看比单看规则更容易记住。**Daft 没有官方镜像**，官方 Helm chart 用的是 Ray 官方镜像 + 运行时装 Daft，细节见 [`examples/quickstart/README.md`](examples/quickstart/README.md)。

### 生产参考清单

[`examples/kuberay/`](examples/kuberay/) 把一个真实跑过的音频流水线压测，按官方 [RayJob Quickstart](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html) 的形态从"常驻 RayCluster + 外部 submit Job"改造过来。参数不是示意值（`10 × 2C/10Gi`、`object-store-memory 2Gi`、`/dev/shm 3Gi`、`asr-actor-concurrency 16`、`default_morsel_size 8`）。

| 文件 | 用途 |
|---|---|
| `00-platform.yaml` | 常驻依赖：namespace、Secret、`bench-env`、`bench-scripts`、MinIO、Mock LLM |
| `05-daft-dashboard.yaml` | 可选，Daft 查询级观测（`:3238`） |
| `10-generate-job.yaml` | 生成输入数据集。普通 `batch/v1` Job —— 单进程任务不该做成 RayJob |
| `20-rayjob.yaml` | **生产默认**：临时集群，`shutdownAfterJobFinishes` |
| `30-raycronjob.yaml` | 可选，每晚小规模回归 |
| `40-raycluster.yaml` + `41-rayjob-existing.yaml` | 可选，常驻集群 + `clusterSelector`，调参用 |

`20` 和 `40`+`41` 抢同一批节点，不要同时 apply。操作步骤和改造对照见 [RayJob 实战](docs/03-best-practices/rayjob-hands-on.md)。apply 前要改三处占位：MinIO 的 `nodeSelector`、镜像 registry、`REPLACE_WITH_DATA_RUN_ID`。
