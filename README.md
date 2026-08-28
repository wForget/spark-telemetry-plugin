# Spark Unified Telemetry Plugin

Spark Driver / Executor 进程内的 fail-open 遥测插件。Metrics、logs、traces 通过 OTLP HTTP/Protobuf 推送到节点本地 Grafana Alloy；profile transport 使用 Pyroscope HTTP Push，并与具体 profiler collector 解耦。

当前实现对应 [ARCHITECTURE.md](ARCHITECTURE.md) 的 Phase 1 核心 MVP：

- Spark `DriverPlugin` / `ExecutorPlugin` 生命周期及 Driver 启动期有界事件桥接
- Spark application、job、stage trace，以及失败/慢任务全保留、普通任务稳定采样的独立 task trace
- job、stage、task、executor 低基数 metrics
- 保留原日志输出的 Log4j2 → OTLP logs bridge，含递归保护和 Spark MDC
- 四信号独立有界异步处理：OTel metrics reader、trace/log batch processor、profile window queue
- profile collector / exporter SPI、Pyroscope HTTP transport、retry 与 circuit breaker
- 统一 Spark 配置、环境变量优先级、严格/按信号 fail-open 校验
- Spark plugin self-metrics、幂等且共享截止时间的有界 shutdown
- OpenTelemetry、Protobuf、OkHttp、Okio、Kotlin 依赖 shade + relocate；Spark/Scala/Log4j/Dropwizard 均由运行时提供

SQL、Structured Streaming、原生 async-profiler/JFR collector、adaptive sampling 和 OTLP Profiles 属于后续阶段。核心包只接收完整 profile window；native collector 应作为独立制品实现 `ProfileCollector`。

## 兼容版本与构建

两个 profile 会生成不同 Scala ABI 的制品，不能混用。构建 Maven 本身请使用 JDK 17；`spark-3.5` 产物仍以 Java 8 字节码编译。

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"

mvn -Pspark-3.5 clean verify
# target/spark-unified-telemetry-plugin-0.1.0-SNAPSHOT-spark-3.5.9_2.12.jar

mvn -Pspark-4.2 clean verify
# target/spark-unified-telemetry-plugin-0.1.0-SNAPSHOT-spark-4.2.0_2.13.jar
```

切换 profile 时必须执行 `clean`，防止 Scala 2.12 / 2.13 的增量编译产物混入。

`ConfigBuilder` / `ConfigEntry` 属于 Spark 私有 ABI，本项目只保证上述两个精确 profile。Spark 的全局 ConfigEntry 注册表会复用同名条目，因此同一 JVM 不应同时加载不同版本的本插件。

## 使用

```bash
spark-submit \
  --jars /path/to/spark-unified-telemetry-plugin-0.1.0-SNAPSHOT-spark-3.5.9_2.12.jar \
  --conf spark.plugins=com.example.spark.telemetry.UnifiedTelemetryPlugin \
  --conf spark.telemetry.endpoint=http://127.0.0.1:4318 \
  --conf spark.telemetry.profile.endpoint=http://127.0.0.1:9999 \
  --conf spark.telemetry.resource.service.name=orders-etl \
  --conf spark.telemetry.resource.service.namespace=data-platform \
  --conf spark.telemetry.resource.deployment.environment=production \
  --class com.example.OrdersJob app.jar
```

OTLP base endpoint 会按信号规范化为 `/v1/metrics`、`/v1/logs`、`/v1/traces`。插件不读取 token、authorization header、TLS private key 等 secret 配置，并拒绝所有 `${env:...}`、`${system:...}` 和 Spark 变量替换表达式，避免解析后的秘密进入 Executor 配置或遥测 Resource；认证与持久重试由 Alloy 管理。

## 配置

所有 key 由 Scala `TelemetryConfig` 使用 Spark `ConfigBuilder` / `ConfigEntry` 统一定义。环境变量名是 Spark key 的大写下划线形式，例如 `spark.telemetry.endpoint` 对应 `SPARK_TELEMETRY_ENDPOINT`。优先级为 packaged defaults < environment < SparkConf < Driver 验证后的 Executor map。时间配置遵循 Spark `timeConf` 语法，支持 `ms`、`s`、`min`、`h` 等单位。

| Key | Default | Description |
|---|---:|---|
| `spark.telemetry.enabled` | `true` | 插件总开关 |
| `spark.telemetry.strict` | `false` | `true` 时非法配置阻止初始化；默认仅关闭受影响信号 |
| `spark.telemetry.endpoint` | `http://127.0.0.1:4318` | Alloy OTLP HTTP base endpoint |
| `spark.telemetry.profile.endpoint` | `http://127.0.0.1:9999` | Alloy Pyroscope receive endpoint |
| `spark.telemetry.metrics.enabled` | `true` | metrics 开关 |
| `spark.telemetry.logs.enabled` | `true` | logs 开关 |
| `spark.telemetry.traces.enabled` | `true` | traces 开关 |
| `spark.telemetry.profiles.enabled` | `false` | profiles 开关；需要外部 collector |
| `spark.telemetry.logs.capture` | `true` | 动态安装 Log4j2 bridge |
| `spark.telemetry.logs.minimum-level` | `INFO` | 最低日志级别 |
| `spark.telemetry.traces.task.sample-rate` | `0.01` | 普通成功任务采样率 |
| `spark.telemetry.traces.slow-task-threshold` | `30s` | 慢任务全保留阈值 |
| `spark.telemetry.profiles.sample-rate` | `19` | collector 建议采样频率 |
| `spark.telemetry.profiles.window` | `10s` | 一个完整 profile window |
| `spark.telemetry.queue.metrics.capacity` | `1000` | metrics 配置上限（SDK 聚合器不按事件排队） |
| `spark.telemetry.queue.logs.capacity` | `10000` | OTel log batch queue |
| `spark.telemetry.queue.traces.capacity` | `5000` | OTel span batch queue |
| `spark.telemetry.queue.profiles.capacity` | `10` | profile window queue |
| `spark.telemetry.batch.max-size` | `512` | 最大导出 batch |
| `spark.telemetry.batch.timeout` | `2s` | batch / metric reader 周期 |
| `spark.telemetry.export.timeout` | `10s` | 单次 exporter timeout |
| `spark.telemetry.shutdown.flush-timeout` | `3s` | 所有信号共享的最终 flush 截止时间 |

Metric Resource 刻意不包含 app/job/stage/task/executor/trace/span ID；它只增加一个不暴露 Spark ID 的 JVM 级 `service.instance.id`，用于避免多个 cumulative writer 相互 reset。若 Alloy 会把该属性提升为长期 Mimir label，应在 Alloy 侧先按目标维度聚合。Trace/log/profile 使用同一规范化身份模型的详细投影。

Executor task span 是独立 trace，并通过 Spark ID 查询关联；公开 Spark Plugin API 无法安全地把 Driver span context 注入每个 task，因此不伪造 parent。为了让执行期间的日志获得真实 trace/span ID，task 开始时会创建 recording span，结束时才根据失败、慢任务和稳定采样规则决定是否送入有界导出队列；因此未保留普通任务的日志可能引用一个未导出的 trace，这是尾部保留策略的预期权衡。
