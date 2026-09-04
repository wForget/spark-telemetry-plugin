# Spark Telemetry Plugin

Spark Driver / Executor 进程内的 fail-open 遥测插件。Metrics、logs、traces 通过 OTLP gRPC 推送到节点本地 Grafana Alloy；continuous profiles 通过 Pyroscope HTTP receiver 推送到 Alloy。

当前实现对应 [ARCHITECTURE.md](ARCHITECTURE.md) 的 Phase 1 核心 MVP：

- Spark `DriverPlugin` / `ExecutorPlugin` 生命周期及 Driver 启动期有界事件桥接
- Spark application、job、stage trace，以及失败/慢任务全保留、普通任务稳定采样的独立 task trace
- Driver / Executor 当前 JVM 中 Spark `MetricsSystem#registry` 的原生指标
- 保留原日志输出的 Log4j2 → OTLP logs bridge，含递归保护和 Spark MDC
- 四信号独立异步处理：OTel metrics reader、trace/log batch processor、Pyroscope Java Agent
- 在拿到真实 app/executor identity 后，以 `PyroscopeAgent.start(Config)` 异步启动 async-profiler
- 可选地在 Executor task 线程安装 `spark_stage_id` / `spark_stage_attempt` 动态 profile 标签
- 统一 Spark 配置、环境变量优先级、严格/按信号 fail-open 校验
- 幂等且共享截止时间的有界 shutdown
- OpenTelemetry、Protobuf、OkHttp、Okio、Kotlin 依赖 shade + relocate；Pyroscope agent 连同 native 库内嵌但保持原包名

SQL、Structured Streaming 和 adaptive sampling 属于后续阶段。

## 兼容版本与构建

两个 profile 会生成不同 Scala ABI 的制品，不能混用。构建 Maven 本身请使用 JDK 17；`spark-3.5` 产物仍以 Java 8 字节码编译。

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"

mvn -Pspark-3.5 clean verify
# target/spark-telemetry-plugin-0.1.0-SNAPSHOT-spark-3.5.9_2.12.jar

mvn -Pspark-4.2 clean verify
# target/spark-telemetry-plugin-0.1.0-SNAPSHOT-spark-4.2.0_2.13.jar
```

切换 profile 时必须执行 `clean`，防止 Scala 2.12 / 2.13 的增量编译产物混入。

`ConfigBuilder` / `ConfigEntry` 以及 `MetricsSystem#registry` 属于 Spark 私有 ABI，本项目只保证上述两个精确 profile。Spark 的全局 ConfigEntry 注册表会复用同名条目，因此同一 JVM 不应同时加载不同版本的本插件。

## 使用

```bash
spark-submit \
  --jars /path/to/spark-telemetry-plugin-0.1.0-SNAPSHOT-spark-3.5.9_2.12.jar \
  --conf spark.plugins=cn.wangz.spark.telemetry.SparkTelemetryPlugin \
  --conf spark.telemetry.endpoint=http://127.0.0.1:4317 \
  --conf spark.telemetry.profiles.enabled=true \
  --conf spark.telemetry.profiles.stage-labels.enabled=true \
  --conf spark.telemetry.profile.endpoint=http://127.0.0.1:9999 \
  --conf spark.telemetry.resource.service.name=orders-etl \
  --conf spark.telemetry.resource.service.namespace=data-platform \
  --conf spark.telemetry.resource.deployment.environment=production \
  --class com.example.OrdersJob app.jar
```

上例的本地绝对路径适用于 client/local 场景。YARN 或 Kubernetes cluster mode 必须改用集群可分发 URI、Spark 的上传配置，或镜像内 `local:///...` 路径，并确保这一插件 JAR 进入 Driver 和所有 Executor 的 classpath；不要用仅分发文件但不加入 classpath 的 `--files`。

Metrics、logs、traces 共享同一个 OTLP gRPC base endpoint，插件不拼接信号路径。endpoint 只允许 `http://` 或 `https://` 的根 URI。插件不读取 token、authorization header、TLS private key 等 secret 配置，并拒绝所有 `${env:...}`、`${system:...}` 和 Spark 变量替换表达式，避免解析后的秘密进入 Executor 配置或遥测 Resource；认证与持久重试由 Alloy 管理。

从旧版升级时，不能继续使用 OTLP/HTTP 的 `4318` endpoint；请改为 Alloy 实际监听的 OTLP gRPC 端口，本地默认为 `4317`。

## Profiling 启动方式

插件使用 Pyroscope 官方 Java API `PyroscopeAgent.start(Config)`，不要求配置 JVM `-javaagent`。固定版本的 agent、其 vendored Java 依赖、JFR 配置和各平台 async-profiler native 库都内嵌在插件 JAR 中；Pyroscope 包保持 `io.pyroscope.*` 原名，绝不 relocation，否则 JNI 绑定会失效。代码启动发生在真实 Spark application ID 已知之后，因此 Driver、Executor profile 能使用与其他信号一致的静态身份标签，agent 链接或启动失败也只会关闭 profiles。

不要再分发独立 Pyroscope JAR，也不要通过 `-javaagent`、应用代码或二次 fat-JAR 重打包引入另一份 `io.pyroscope.*`；当前部署约定运行环境中不存在其他 Pyroscope 类。同一 JVM 若已有 agent，则视为外部所有，不修改也不停止它。Local 模式的 Driver 与 Executor 共用一个 JVM，只由 Driver 启动一次，`spark_role=local_jvm` 表示 profile 同时包含 Driver 和 task 执行。

与 premain 方式相比，代码启动看不到 Spark/JVM 最早启动阶段，也不会执行 Pyroscope 的 bootstrap API 注入；continuous profiling、静态身份标签和插件显式创建的 stage scoped context 不依赖该能力。若后续加入通用跨线程标签传播或 span-profile correlation，需要单独验证或重新评估 `-javaagent`。

`spark.telemetry.profiles.stage-labels.enabled=true` 时，Executor 在每个 task 开始和结束之间给当前线程安装 `spark_stage_id` 与 `spark_stage_attempt` 动态标签。它允许按 app/stage 聚合多个 Executor 的 profile，但不会把标签传播到 GC、shuffle helper 等后台线程；短于采样间隔的 task 也可能没有样本。该选项默认关闭，因为长时间运行的 Structured Streaming 应用会持续产生 stage ID，增加 Pyroscope 标签基数；客户端还会把已关闭 context 的元数据暂存到下一个 profile dump，峰值约随 task 速率 × upload interval 增长。Local 模式仍只由 Driver 管理 agent 生命周期，Executor 在标准同一 classloader 部署下切换 task scoped context；若 Spark 隔离了插件 classloader，Executor 会安全退化为无 stage 标签。

仓库自带的 `Spark Application Detail` dashboard 在 flamegraph 上提供 profile type、`Profile stage ID (regex)` 和 `Profile stage attempt (regex)` 选择器，stage/attempt 默认 `.*` 保留全部样本；输入 `91` / `0` 即可只看 stage 91 的首次尝试。`profiles.event=CPU|ITIMER` 使用默认的 `CPU` profile type，`profiles.event=WALL` 需在 dashboard 中选择 `Wall`。在 Grafana Explore 中也可直接使用 `{service_name="orders-etl",spark_app_id="application-123",spark_stage_id="91",spark_stage_attempt="0"}` 作为 Pyroscope label selector，避免同一 service 的多次 application 运行相互混合。

默认事件是无需 Linux `perf_event_open` 权限的 `itimer`。若运行环境已验证 perf 权限，可设置 `spark.telemetry.profiles.event=cpu` 使用硬件 CPU 事件并同时采集 Java、JVM、kernel 与 native 调用栈；native 栈展开可通过 `spark.telemetry.profiles.async-profiler.extra-arguments=cstack=dwarf,memlimit=128m` 调整。Allocation 和 lock profiling 默认关闭，分别通过 `profiles.alloc`、`profiles.lock` 显式开启。为限制 native CPU/内存和信号风险，extra arguments 只允许 `cstack=fp|dwarf|vm|vmx|no` 与 `memlimit=16m..1g`；未指定 memlimit 时自动追加 `128m`。

Profile startup 涉及 native 库提取和加载，JVM `java.io.tmpdir` 需要可写且可执行；`/tmp` 为 `noexec`、不支持的 libc/CPU/JVM 或 native profiler crash 都需要在目标镜像和 GC 组合上 canary。Java 异常可以 fail-open，但 native SIGSEGV 无法由插件恢复。`PyroscopeAgent.stop()` 不保证导出当前窗口或 drain 上传队列，因此短于首个上传窗口的作业可能没有 profile，Executor 回收时也可能丢失最后一个窗口。

## 配置

所有 key 由 Scala `TelemetryConfig` 使用 Spark `ConfigBuilder` / `ConfigEntry` 统一定义。环境变量名是 Spark key 的大写下划线形式，例如 `spark.telemetry.endpoint` 对应 `SPARK_TELEMETRY_ENDPOINT`。优先级为 packaged defaults < environment < SparkConf < Driver 验证后的 Executor map。时间配置遵循 Spark `timeConf` 语法，支持 `ms`、`s`、`min`、`h` 等单位。

| Key | Default | Description |
|---|---:|---|
| `spark.telemetry.enabled` | `true` | 插件总开关 |
| `spark.telemetry.strict` | `false` | `true` 时非法配置阻止初始化；默认仅关闭受影响信号 |
| `spark.telemetry.endpoint` | `http://127.0.0.1:4317` | Alloy OTLP gRPC base endpoint |
| `spark.telemetry.metrics.enabled` | `true` | metrics 开关 |
| `spark.telemetry.logs.enabled` | `true` | logs 开关 |
| `spark.telemetry.traces.enabled` | `true` | traces 开关 |
| `spark.telemetry.profiles.enabled` | `false` | continuous profiles 开关；agent 已内嵌在插件 JAR |
| `spark.telemetry.profiles.stage-labels.enabled` | `false` | 为 Executor task profile 添加可筛选的 stage ID/attempt 动态标签 |
| `spark.telemetry.profile.endpoint` | `http://127.0.0.1:9999` | Alloy Pyroscope HTTP base endpoint |
| `spark.telemetry.profiles.event` | `ITIMER` | 主采样事件：`ITIMER`、`CPU`、`WALL` |
| `spark.telemetry.profiles.interval` | `10ms` | async-profiler 采样间隔，范围 `5ms..1s` |
| `spark.telemetry.profiles.upload-interval` | `10s` | 采样窗口/上传周期，范围 `2s..5min` |
| `spark.telemetry.profiles.java-stack-depth` | `2048` | 最大 Java 栈深度，范围 `64..4096` |
| `spark.telemetry.profiles.alloc` | 空 | allocation sampling interval，范围 `512k..1g` |
| `spark.telemetry.profiles.lock` | 空 | lock profiling threshold，范围 `1ms..60s` |
| `spark.telemetry.profiles.async-profiler.extra-arguments` | `memlimit=128m` | 额外 async-profiler 参数，例如 `cstack=dwarf,memlimit=128m` |
| `spark.telemetry.logs.capture` | `true` | 动态安装 Log4j2 bridge |
| `spark.telemetry.logs.minimum-level` | `ERROR` | 最低日志级别 |
| `spark.telemetry.traces.task.sample-rate` | `0.01` | 普通成功任务采样率 |
| `spark.telemetry.traces.slow-task-threshold` | `30s` | 慢任务全保留阈值 |
| `spark.telemetry.queue.logs.capacity` | `10000` | OTel log batch queue |
| `spark.telemetry.queue.traces.capacity` | `5000` | OTel span batch queue |
| `spark.telemetry.queue.profiles.capacity` | `10` | Pyroscope agent 上传队列，范围 `1..64` |
| `spark.telemetry.batch.max-size` | `512` | 最大导出 batch |
| `spark.telemetry.batch.timeout` | `2s` | batch / metric reader 周期 |
| `spark.telemetry.export.timeout` | `10s` | 单次 exporter timeout |
| `spark.telemetry.shutdown.flush-timeout` | `3s` | 所有信号共享的最终 flush 截止时间 |

Metric Resource 刻意不包含 app/job/stage/task/executor/trace/span ID；它只增加一个不暴露 Spark ID 的 JVM 级 `service.instance.id`，用于避免多个 cumulative writer 相互 reset。每个 Metric DataPoint 包含进程生命周期内稳定的 `spark.executor.id` 查询维度，Alloy 的 Prometheus exporter 会将其规范化为 Mimir label `spark_executor_id`，而不必把其他 Resource 属性全部提升成时序标签。Trace/log 使用同一规范化身份模型的详细投影。

Metrics pipeline 不创建或维护插件指标，也不再从 Spark listener / task callback 记录 job、stage、task、executor 指标。每个导出周期直接读取当前 JVM 的 Spark Dropwizard registry，因此 Spark 后续动态注册的指标也会被投递；非数值 Gauge 会被忽略，单个 Gauge 读取失败不会影响其余指标。Local 模式的 Driver 和 Executor 共享同一个 `MetricsSystem`，只由 Driver runtime 投递一次。

Executor task span 是独立 trace，并通过 Spark ID 查询关联；公开 Spark Plugin API 无法安全地把 Driver span context 注入每个 task，因此不伪造 parent。为了让执行期间的日志获得真实 trace/span ID，task 开始时会创建 recording span，结束时才根据失败、慢任务和稳定采样规则决定是否送入有界导出队列；因此未保留普通任务的日志可能引用一个未导出的 trace，这是尾部保留策略的预期权衡。失败 task span 设置 `ERROR` status 和结构化 Spark failure 属性；`ExceptionFailure` 使用标准 OTel `exception` event，Spark 无法保留原始 Throwable 时则从其保留的类型、消息和完整堆栈构造等价事件。非异常失败原因使用 `spark.task.failure` event，不伪造 Throwable。

Driver 在收到 stage attempt 完成事件时，把 Spark 此刻暴露的 task metrics 快照写入 `spark.stage` span：`spark.stage.task_metrics.executor_run_time_ms`、`memory_bytes_spilled`、`disk_bytes_spilled`、`input.bytes_read`、`output.bytes_written`、`shuffle.read.total_bytes_read`、`shuffle.read.fetch_wait_time_ms`、`shuffle.write.bytes_written` 和 `shuffle.write.write_time_ns`。时间值是 task 时间之和而非 stage 墙钟时间，可能因并行执行而大于 span duration；同一 stage attempt 内已被 Spark 合并的失败、重试或被终止 task 开销也可能计入，但并非所有失败原因都会提供 accumulator update。Spark 异步投递 listener 事件且旧 task 更新存在时序差异，因此这些值是近似累计开销，不是事务性冻结的 terminal totals。`StageInfo.taskMetrics` 不可用时整组属性省略，非适用但可用的指标保留 Spark 返回的零值。

为了复现 Spark UI 的 task time 分解，Driver 还在 `SparkListenerTaskEnd` 上逐 task 计算后按 `(stageId, stageAttemptId)` 累加 Scheduler Delay、Task Deserialization、Shuffle Read、Executor Computing、Shuffle Write、Result Serialization 和 Getting Result，分别写入 `spark.stage.task_metrics.timeline.scheduler_delay_ms`、`executor_deserialize_time_ms`、`shuffle_read_time_ms`、`executor_computing_time_ms`、`shuffle_write_time_ms`、`result_serialization_time_ms` 和 `getting_result_time_ms`。该组仅包含 stage 完成前观察到且带 `TaskMetrics` 的 task attempt；同一命名空间下的 `observed_task_attempts` 与 `included_task_attempts` 用于判断覆盖度，完全没有可用 task metrics 时整组省略。Application Detail 的 `Stage task time distribution` 以普通堆叠条形图显示这七段累计工作时间，并显式区分 stage attempt；Stage 标签以 `tasks included/observed` 展示覆盖度。它不是实际时序或 stage 关键路径，Shuffle Read 具体表示 fetch wait，Executor Computing 仍包含 GC 等 executor 工作。面板通过 Tempo span-table 查询，`spss=100` 将每个返回 span set 中匹配的 stage span 限制为最多 100 个。
