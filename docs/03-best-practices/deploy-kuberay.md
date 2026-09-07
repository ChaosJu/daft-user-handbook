# KubeRay 部署

选型其实只有两个问题：

```text
问题一   driver 放在哪？        → 决定版本耦合、断线影响、故障域
问题二   集群谁创建、谁回收？    → 决定成本、环境一致性、能不能定时
```

生产批处理默认选 **RayJob 临时集群（B）**。调参与共享开发用 **常驻 RayCluster（A）**。同一套节点上不要同时跑 A 和 B。

## 方案对照

| 组合 | driver 在哪 | 集群谁管 | 定位 |
|---|---|---|---|
| Native runner（不用 Ray） | 本地进程 | 无集群 | 单机调试、正确性验证 |
| Ray Client `ray://` | **集群外** | 人工 | 交互开发，**不适合长任务** |
| **常驻 RayCluster + Jobs API（A）** | 集群内 head | 人工建、人工删 | 长期在线的共享集群 |
| **RayJob 自带临时集群（B）** | 集群内 head | **operator 建、自动删** | **生产批处理的默认选择** |
| RayCronJob | 集群内 head | operator 按 schedule | 周期性批处理 |

每一行都有必须先认下来的代价：

```text
Native runner      不能验证分布式调度、资源约束与故障语义
Ray Client         版本必须严格一致；长连接一断作业就失败
常驻 RayCluster    持续占资源；多作业争 CPU、object store 与配额
RayJob 临时集群    冷启动更慢（镜像、依赖、模型）；日志与产物必须在回收前外送
RayCronJob         重试由谁发起、幂等 run id 怎么保持、终态谁回收，都要先定归属
```

**默认选 B**：生产批处理、周期任务、需要严格环境隔离的作业。
**用 A**：调参、共享开发集群、对启动延迟敏感的密集作业。

## 角色：谁在哪台机器上

```text
client    构造 DataFrame / SQL，只产出 LogicalPlan，不执行
driver    优化计划、切 task、派发；Daft 的 Flotilla 在这里
head      GCS + Dashboard / Jobs API；driver 通常也在这个 Pod 里
worker    raylet + object store + Swordfish + UDF actor，真正干活
operator  只管 Pod 该不该存在，不参与任务调度
```

**数据不经过 client，也不经过 driver。** worker 直连对象存储读写，driver 手里只有 metadata 和 ObjectRef。这决定了后面所有的资源判断：给 head 加内存解决不了 worker OOM，给 client 加带宽也加速不了扫描。

三层口径必须对齐：K8s 认 `limits`，Ray 认 `num-cpus`，Daft 认 partition 与 morsel。第 1 章那三条硬约束，本质就是要求这三套口径一致。

## KubeRay 的四个 CRD

| CRD | 声明什么 | 生命周期 | 批处理里的位置 |
|---|---|---|---|
| **RayCluster** | head 与 worker 组的形状 | 一直在，直到被 delete | 常驻共享集群（A） |
| **RayJob** | 一次作业，可自带 `rayClusterSpec` | 终态后可自动回收 | **默认选择（B）** |
| RayCronJob | `{schedule, jobTemplate}` | 按 cron 反复创建 RayJob | 周期任务（Alpha，需 feature gate） |
| RayService | Ray Serve 应用与滚动升级 | 常驻服务 | **不要拿它跑批** |

RayJob 有两种用法，区别只在有没有 `rayClusterSpec`：

```text
写了 rayClusterSpec     → operator 现拉一套临时集群，跑完回收
只给 clusterSelector    → 提交到已存在的 RayCluster 上，不管生命周期
```

所以 **RayJob ≠ 临时集群**。它是作业语义这一层；套在临时集群上是常见用法，不是唯一用法。

## A 与 B：好处与代价

| | A 常驻 RayCluster | B RayJob 临时集群 |
|---|---|---|
| 集群生命周期 | 常驻，人工删 | 每作业一套，自动回收 |
| 空转成本 | 不跑作业也占着资源 | 零空转 |
| 启动延迟 | 低，镜像与模型已在节点上 | 每次冷启动：拉镜像、装载模型 |
| 环境隔离 | 多作业共享，争 CPU 与 object store | 每次全新，无跨作业残留 |
| Dashboard | 一直可看，能回看历史作业 | 只在 TTL 窗口内 |
| 产物 | 写在 Pod 内的随重建消失 | 必须在集群销毁前送出去 |
| 可审计 | 靠外部记录 | 一个 CR 就是一次作业的完整记录 |
| 超时与清理 | 自己写脚本 | operator 兜 |

同一套资源上 A 与 B **不要同时存在**——两份 CR 会去抢同一批节点。

要定时就在 B 外包一层 `RayCronJob`；已有 Argo / Airflow 的话让它们创建 RayJob 等价，区别只在调度器在 K8s 内还是外。

## YAML：RayCluster head

head 不接计算任务。`num-cpus: "0"` 是硬约束。

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: daft
spec:
  rayVersion: "2.55.1"
  headGroupSpec:
    serviceType: ClusterIP
    rayStartParams:
      num-cpus: "0"
      dashboard-host: "0.0.0.0"
      metrics-export-port: "8080"
    template:
      spec:
        containers:
          - name: ray-head
            image: my-registry/daft-runtime:1.0.0
            ports:
              - { name: metrics, containerPort: 8080 }
              - { name: as-metrics, containerPort: 44217 }
              - { name: dash-metrics, containerPort: 44227 }
            resources:
              requests: { cpu: "2", memory: 8Gi }
              limits:   { cpu: "2", memory: 8Gi }
            volumeMounts:
              - { name: dshm, mountPath: /dev/shm }
            lifecycle:
              preStop: { exec: { command: ["/bin/sh", "-c", "ray stop"] } }
        volumes:
          - name: dshm
            emptyDir: { medium: Memory, sizeLimit: 2Gi }
```

整份 CR 里没有一个字段和作业有关——它只回答“集群长什么样”。

## YAML：CPU worker group

worker 才是资源大头。`replicas` / `minReplicas` / `maxReplicas` 三者相等 = 明确关掉 autoscaling。

```yaml
  workerGroupSpecs:
    - groupName: cpu
      replicas: 8
      minReplicas: 8
      maxReplicas: 8
      rayStartParams:
        num-cpus: "8"                          # 必须 == limits.cpu
        object-store-memory: "4000000000"
        metrics-export-port: "8080"
      template:
        spec:
          containers:
            - name: ray-worker
              image: my-registry/daft-runtime:1.0.0
              ports:
                - { name: metrics, containerPort: 8080 }
              resources:
                requests: { cpu: "8", memory: 48Gi }
                limits:   { cpu: "8", memory: 48Gi }
              volumeMounts:
                - { name: dshm, mountPath: /dev/shm }
          volumes:
            - name: dshm
              emptyDir: { medium: Memory, sizeLimit: 8Gi }  # 实际用量计入 48Gi
```

落地三条硬约束：`num-cpus == limits.cpu`、`requests == limits`、`/dev/shm` 从内存 limit 里切出来。

## YAML：GPU worker group

结构与 CPU 组相同，另外多四处：

```yaml
    - groupName: gpu
      replicas: 2
      minReplicas: 2
      maxReplicas: 2
      rayStartParams:
        num-cpus: "8"
        num-gpus: "1"
        metrics-export-port: "8080"
      template:
        spec:
          nodeSelector: { accelerator: nvidia }
          tolerations:
            - { key: nvidia.com/gpu, operator: Exists, effect: NoSchedule }
          containers:
            - name: ray-worker
              image: my-registry/daft-runtime:1.0.0
              resources:
                requests: { cpu: "8", memory: 64Gi, nvidia.com/gpu: "1" }
                limits:   { cpu: "8", memory: 64Gi, nvidia.com/gpu: "1" }
```

```text
nodeSelector + toleration   把这组钉到 GPU 节点池
num-gpus 上报给 Ray         @daft.cls(gpus=1) 才有资源可申请
GPU 卡数不可超卖             一张卡同时只服务一个 actor，靠 replicas 扩
```

## YAML：RayJob 临时集群

作业语义是 RayCluster 没有的东西：入口、超时、重试、回收。

```yaml
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: daft-nightly
spec:
  entrypoint: /opt/venv/bin/python /app/pipeline.py --num-partitions 64
  entrypointNumCpus: 0
  submissionMode: K8sJobMode
  shutdownAfterJobFinishes: true
  ttlSecondsAfterFinished: 600
  activeDeadlineSeconds: 21600
  preRunningDeadlineSeconds: 1800
  backoffLimit: 0
  rayClusterSpec:
    # 内容逐字段等于上面那份 RayCluster 的 spec
```

完整可 apply 清单见 [`examples/kuberay/20-rayjob.yaml`](../../examples/kuberay/20-rayjob.yaml)（临时集群）与 [`40-raycluster.yaml`](../../examples/kuberay/40-raycluster.yaml) + [`41-rayjob-existing.yaml`](../../examples/kuberay/41-rayjob-existing.yaml)（常驻集群 + `clusterSelector`）。按官方步骤走一遍见 [RayJob 实战](rayjob-hands-on.md)。

`rayClusterSpec` 等于把 RayCluster 的 `spec` 整段搬过来。从 A 迁到 B 不用重新设计集群。

约束：

- `ttlSecondsAfterFinished > 0` 要求 `shutdownAfterJobFinishes: true`
- `backoffLimit` 默认 0；每次重试都会新拉一套集群
- `suspend: true` 会删掉已建集群

RayJob 提交到已有集群：

```yaml
spec:
  entrypoint: /opt/venv/bin/python /app/pipeline.py
  clusterSelector:
    ray.io/cluster: daft
  # 与 rayClusterSpec 二选一
```

这种模式下 KubeRay **不会**去删别人的集群。

## 作业怎么进集群

| | Ray Client | Jobs API | RayJob CR |
|---|---|---|---|
| driver 位置 | 集群外 | head | head |
| 版本要求 | client 与集群必须严格一致 | 只要 8265 可达 | 同 Jobs API |
| 断线影响 | 作业立即死 | 无 | 无 |
| 依赖交付 | 靠本地环境 | 镜像或 `working_dir` | 镜像 |
| 集群生命周期 | 人工 | 人工 | 声明式，可自动回收 |
| 适合 | 交互开发 | 脚本或平台触发的批处理 | 生产与平台化托管 |

> **除交互调试外，不要在生产链路上用 Ray Client。**
> 它把 driver 留在集群外，等于把作业的生死绑在一条长连接和一台开发机上。

Jobs API 提交示例（常驻集群 A）：

```bash
ray job submit --address http://daft-head-svc:8265 \
  --working-dir . -- python pipeline.py --num-partitions 64
```

## Airflow / 数据平台：选哪个 `submissionMode`

```text
平台管 CR     平台 apply RayJob，再 watch CR status   → K8sJobMode / SidecarMode
平台管提交    平台自己调 Jobs API，RayJob 只管集群     → InteractiveMode
```

| mode | 额外 Pod | 日志出口 | 适合 |
|---|---|---|---|
| **K8sJobMode**（默认） | 1 个 submitter Job | submitter Pod 日志里就有 driver 输出 | **Airflow / Argo 的默认选择** |
| **SidecarMode**（KubeRay 1.5） | 0，跑在 head Pod 内 | head 里的 sidecar 容器 | 并发几十上百个作业，省 Pod |
| HTTPMode | 0，operator 直接调 | 无 submitter 日志 | 轻量触发 |
| InteractiveMode | 0 | 平台自己的通道 | 平台已有统一 SDK |

**用 Airflow 就选默认的 K8sJobMode**——标准 Pod 日志出口，Airflow 抓 task log 不用另接 Dashboard。作业量大到 Pod 数成为负担，再换 SidecarMode。

## 不要做的两件事

1. **生产不要用 Ray Client。** 长连接、版本耦合、driver 在集群外。
2. **不要在同一批节点上混跑常驻 RayCluster 和 RayJob 临时集群。** 两套 CR 会抢资源，指标和配额都会乱。

RayJob 必须带超时和 TTL，否则失败集群会一直占着节点。日志必须在 TTL 到期前外送到 Loki / ES / 对象存储，否则现场随集群一起消失。
