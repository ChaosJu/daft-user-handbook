# Daft 用户指导手册

面向生产使用的 **Daft on Ray** 手册：架构、执行原理、KubeRay 部署实践、运维调参，外加一套可以直接 `kubectl apply` 的示例清单。

本手册不是 API 百科，写的是生产共识。官方 API 以 [docs.daft.ai](https://docs.daft.ai) 为准。

**从 [`docs/index.md`](docs/index.md) 开始读**，那里有十页导航和四条要先记住的规则。要直接在集群上跑，跳到 [KubeRay RayJob 部署](docs/03-deploy-kuberay.md)。

## 示例清单

三个目录，都是 RayJob 形态，区别只在"跑什么、用什么镜像"：

| | [`examples/docker/`](examples/docker/) | [`examples/quickstart/`](examples/quickstart/) | [`examples/kuberay/`](examples/kuberay/) |
|---|---|---|---|
| 是什么 | 镜像构建 | 最小可跑示例 | 生产参考 |
| 镜像 | 产出 `daft-ray-ops` | 用 `daft-ray-ops` | `daft-audio:offline`（ASR 专用） |
| 业务 | — | 六行数据 `where` + `sort` | 真实音频 ASR 流水线 |
| 规模 | — | 1 worker × 2C/4Gi | 10 worker × 2C/10Gi |
| 外部依赖 | — | 无 | MinIO + Mock LLM |
| 用途 | 依赖烤进镜像，离线可用 | 验证 KubeRay 链路通不通 | 抄参数、抄结构 |

依赖关系是 `docker` → `quickstart` → `kuberay`：先构建镜像，再跑最小示例确认 KubeRay 装对了，最后照着生产清单改自己的。

### 生产参考清单

[`examples/kuberay/`](examples/kuberay/) 是一个真实跑过的音频流水线压测，按官方 [RayJob Quickstart](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html) 的形态组织。参数不是示意值（`10 × 2C/10Gi`、`object-store-memory 2Gi`、`/dev/shm 3Gi`、`asr-actor-concurrency 16`、`default_morsel_size 8`）。

| 文件 | 用途 |
|---|---|
| `00-platform.yaml` | 常驻依赖：namespace、Secret、`bench-env`、`bench-scripts`、MinIO、Mock LLM |
| `05-daft-dashboard.yaml` | 可选，Daft 查询级观测（`:3238`） |
| `10-generate-job.yaml` | 生成输入数据集。普通 `batch/v1` Job —— 单进程任务不该做成 RayJob |
| `20-rayjob.yaml` | **生产默认**：临时集群，`shutdownAfterJobFinishes` |
| `30-raycronjob.yaml` | 可选，每晚小规模回归 |
| `40-raycluster.yaml` + `41-rayjob-existing.yaml` | 可选，常驻集群 + `clusterSelector`，调参用 |

`20` 和 `40`+`41` 抢同一批节点，不要同时 apply。apply 前要改三处占位：MinIO 的 `nodeSelector`、镜像 registry、`REPLACE_WITH_DATA_RUN_ID`。步骤见 [KubeRay RayJob 部署](docs/03-deploy-kuberay.md)。

## 本地预览文档

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux / macOS

pip install -r requirements.txt
mkdocs serve
```

浏览器打开 <http://127.0.0.1:8000>。构建静态站点用 `mkdocs build`，产物在 `site/`。
