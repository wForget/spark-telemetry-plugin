package org.apache.spark.telemetry.config

import java.net.URI
import java.time.Duration
import java.util.{Collections, LinkedHashMap, Locale, Map => JMap}
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, ConfigProvider, ConfigReader}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Immutable, validated source of truth for every plugin setting. */
final class TelemetryConfig private (
    private val conf: SparkConf,
    private val executorConfiguration: JMap[String, String]) {

  import TelemetryConfig._

  def enabled(): Boolean = conf.get(ENABLED)
  def strict(): Boolean = conf.get(STRICT)
  def metricsEnabled(): Boolean = enabled() && conf.get(METRICS_ENABLED)
  def logsEnabled(): Boolean = enabled() && conf.get(LOGS_ENABLED)
  def tracesEnabled(): Boolean = enabled() && conf.get(TRACES_ENABLED)
  def logCaptureEnabled(): Boolean = logsEnabled() && conf.get(LOG_CAPTURE)
  def endpoint(): String = conf.get(ENDPOINT)
  def minimumLogLevel(): TelemetryLogLevel = TelemetryLogLevel.valueOf(conf.get(LOG_MINIMUM_LEVEL))
  def taskSampleRate(): Double = conf.get(TASK_SAMPLE_RATE)
  def slowTaskThreshold(): Duration = Duration.ofMillis(conf.get(SLOW_TASK_THRESHOLD))
  def logsQueueCapacity(): Int = conf.get(LOGS_QUEUE_CAPACITY)
  def tracesQueueCapacity(): Int = conf.get(TRACES_QUEUE_CAPACITY)
  def batchMaxSize(): Int = conf.get(BATCH_MAX_SIZE)
  def batchTimeout(): Duration = Duration.ofMillis(conf.get(BATCH_TIMEOUT))
  def exportTimeout(): Duration = Duration.ofMillis(conf.get(EXPORT_TIMEOUT))
  def shutdownFlushTimeout(): Duration = Duration.ofMillis(conf.get(SHUTDOWN_FLUSH_TIMEOUT))
  def serviceName(): String = conf.get(SERVICE_NAME)
  def serviceNamespace(): String = conf.get(SERVICE_NAMESPACE)
  def deploymentEnvironment(): String = conf.get(DEPLOYMENT_ENVIRONMENT)
  def cluster(): String = conf.get(CLUSTER)
  def applicationName(): String = conf.get(INTERNAL_APP_NAME)
  def applicationId(): String = conf.get(INTERNAL_APP_ID)

  def withApplication(appName: String, appId: String): TelemetryConfig = {
    val enriched = conf.clone()
    enriched.set(INTERNAL_APP_NAME, emptyTo(appName, "spark"))
    enriched.set(INTERNAL_APP_ID, emptyTo(appId, "unknown"))
    if (enriched.get(SERVICE_NAME) == "spark") {
      enriched.set(SERVICE_NAME, emptyTo(appName, "spark"))
    }
    freeze(enriched)
  }

  def toExecutorConfiguration(): JMap[String, String] = executorConfiguration

  def otlpSignalEndpoint(signal: String): String = {
    val standardSignals = Array("metrics", "logs", "traces")
    var base = trimTrailingSlash(endpoint())
    standardSignals.iterator.map("/v1/" + _).find(base.endsWith).foreach { suffix =>
      base = base.substring(0, base.length - suffix.length)
    }
    trimTrailingSlash(base) + "/v1/" + signal
  }
}

/** Spark-style declarations and construction for [[TelemetryConfig]]. */
object TelemetryConfig {
  private val Prefix = "spark.telemetry."
  private val Version = "0.1.0"

  val ENABLED: ConfigEntry[Boolean] =
    boolean("enabled", "Enable the telemetry plugin", default = true)
  val STRICT: ConfigEntry[Boolean] =
    boolean("strict", "Fail Spark initialization when telemetry configuration is invalid", default = false)
  val ENDPOINT: ConfigEntry[String] =
    text("endpoint", "Alloy OTLP HTTP base endpoint", "http://127.0.0.1:4318")

  val METRICS_ENABLED: ConfigEntry[Boolean] = signalEnabled("metrics")
  val LOGS_ENABLED: ConfigEntry[Boolean] = signalEnabled("logs")
  val TRACES_ENABLED: ConfigEntry[Boolean] = signalEnabled("traces")

  val LOG_MINIMUM_LEVEL: ConfigEntry[String] =
    define("logs.minimum-level", "Minimum captured log level") { builder =>
      builder.stringConf
        .transform(_.trim.toUpperCase(Locale.ROOT))
        .checkValues(Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"))
        .createWithDefault("INFO")
    }
  val LOG_CAPTURE: ConfigEntry[Boolean] =
    boolean("logs.capture", "Install the non-destructive Log4j2 telemetry bridge", default = true)
  val TASK_SAMPLE_RATE: ConfigEntry[Double] =
    define("traces.task.sample-rate", "Stable sample rate for normal successful tasks") { builder =>
      builder.doubleConf
        .checkValue(value => !value.isNaN && value >= 0.0d && value <= 1.0d, "must be in [0, 1]")
        .createWithDefault(0.01d)
    }
  val SLOW_TASK_THRESHOLD: ConfigEntry[Long] =
    time("traces.slow-task-threshold", "Threshold above which a task trace is retained", "30s")

  val LOGS_QUEUE_CAPACITY: ConfigEntry[Int] =
    positiveInt("queue.logs.capacity", "OTel log batch queue capacity", 10000)
  val TRACES_QUEUE_CAPACITY: ConfigEntry[Int] =
    positiveInt("queue.traces.capacity", "OTel span batch queue capacity", 5000)
  val BATCH_MAX_SIZE: ConfigEntry[Int] =
    positiveInt("batch.max-size", "Maximum export batch size", 512)
  val BATCH_TIMEOUT: ConfigEntry[Long] =
    time("batch.timeout", "Batch and metric reader interval", "2s")
  val EXPORT_TIMEOUT: ConfigEntry[Long] =
    time("export.timeout", "Timeout for one export attempt", "10s")
  val SHUTDOWN_FLUSH_TIMEOUT: ConfigEntry[Long] =
    time("shutdown.flush-timeout", "Shared final flush deadline", "3s")

  val SERVICE_NAME: ConfigEntry[String] =
    text("resource.service.name", "OTel service name", "spark")
  val SERVICE_NAMESPACE: ConfigEntry[String] =
    text("resource.service.namespace", "OTel service namespace", "")
  val DEPLOYMENT_ENVIRONMENT: ConfigEntry[String] =
    text("resource.deployment.environment", "Deployment environment", "")
  val CLUSTER: ConfigEntry[String] =
    text("resource.cluster", "Spark cluster name", "")

  private val INTERNAL_APP_NAME: ConfigEntry[String] =
    internalText("internal.app-name", "Driver-validated Spark application name", "spark")
  private val INTERNAL_APP_ID: ConfigEntry[String] =
    internalText("internal.app-id", "Driver-validated Spark application id", "unknown")

  private val UserEntries: Seq[ConfigEntry[_]] = Seq(
    ENABLED, STRICT, ENDPOINT,
    METRICS_ENABLED, LOGS_ENABLED, TRACES_ENABLED,
    LOG_MINIMUM_LEVEL, LOG_CAPTURE, TASK_SAMPLE_RATE, SLOW_TASK_THRESHOLD,
    LOGS_QUEUE_CAPACITY, TRACES_QUEUE_CAPACITY,
    BATCH_MAX_SIZE, BATCH_TIMEOUT, EXPORT_TIMEOUT,
    SHUTDOWN_FLUSH_TIMEOUT, SERVICE_NAME, SERVICE_NAMESPACE,
    DEPLOYMENT_ENVIRONMENT, CLUSTER)
  private val AllEntries: Seq[ConfigEntry[_]] = UserEntries ++ Seq(INTERNAL_APP_NAME, INTERNAL_APP_ID)
  private val UserEntryKeys: Set[String] = UserEntries.iterator.map(_.key).toSet
  private val EntryKeys: Set[String] = AllEntries.iterator.map(_.key).toSet

  def from(sparkConf: SparkConf): TelemetryConfig = from(sparkConf, System.getenv())

  def from(sparkConf: SparkConf, environment: JMap[String, String]): TelemetryConfig = {
    val merged = new SparkConf(false)
    copyEnvironment(environment, merged)
    copyKnown(sparkConf.getAll.iterator, merged, UserEntryKeys)
    validateAndFreeze(merged)
  }

  def from(sparkConfiguration: JMap[String, String]): TelemetryConfig =
    from(sparkConfiguration, System.getenv())

  def from(
      sparkConfiguration: JMap[String, String],
      environment: JMap[String, String]): TelemetryConfig = {
    val merged = new SparkConf(false)
    copyEnvironment(environment, merged)
    if (sparkConfiguration != null) copyKnown(sparkConfiguration.asScala.iterator, merged, UserEntryKeys)
    validateAndFreeze(merged)
  }

  /** Rebuild exactly the Driver-validated configuration on an Executor. */
  def fromDriver(driverConfiguration: JMap[String, String]): TelemetryConfig = {
    if (driverConfiguration == null || driverConfiguration.isEmpty) return disabled()
    val merged = new SparkConf(false)
    copyKnown(driverConfiguration.asScala.iterator, merged, EntryKeys)
    validateAndFreeze(merged)
  }

  /** Safe fallback when configuration or a provided dependency cannot be read. */
  def disabled(): TelemetryConfig = {
    val conf = new SparkConf(false)
    conf.set(ENABLED, false)
    freeze(conf)
  }

  def strictRequested(conf: SparkConf, fallback: Boolean): Boolean =
    try value(conf, STRICT) catch { case NonFatal(_) => fallback }

  def strictRequested(values: JMap[String, String], fallback: Boolean): Boolean = {
    if (values == null) return fallback
    val raw = values.get(STRICT.key)
    if (raw == null) fallback else raw.trim.equalsIgnoreCase("true")
  }

  private def define[T](
      suffix: String,
      documentation: String)(create: ConfigBuilder => ConfigEntry[T]): ConfigEntry[T] = {
    val key = Prefix + suffix
    ConfigEntry.synchronized {
      val existing = ConfigEntry.findEntry(key)
      if (existing != null) existing.asInstanceOf[ConfigEntry[T]]
      else create(ConfigBuilder(key).doc(documentation).version(Version))
    }
  }

  private def signalEnabled(signal: String): ConfigEntry[Boolean] =
    boolean(signal + ".enabled", "Enable " + signal + " export", default = true)

  private def boolean(
      suffix: String,
      documentation: String,
      default: Boolean): ConfigEntry[Boolean] =
    define(suffix, documentation)(_.booleanConf.createWithDefault(default))

  private def text(
      suffix: String,
      documentation: String,
      default: String): ConfigEntry[String] =
    define(suffix, documentation)(_.stringConf.transform(_.trim).createWithDefault(default))

  private def internalText(
      suffix: String,
      documentation: String,
      default: String): ConfigEntry[String] =
    define(suffix, documentation)(_.internal().stringConf.transform(_.trim).createWithDefault(default))

  private def positiveInt(
      suffix: String,
      documentation: String,
      default: Int): ConfigEntry[Int] =
    define(suffix, documentation) { builder =>
      builder.intConf.checkValue(_ > 0, "must be greater than zero").createWithDefault(default)
    }

  private def time(
      suffix: String,
      documentation: String,
      default: String): ConfigEntry[Long] =
    define(suffix, documentation) { builder =>
      builder.timeConf(TimeUnit.MILLISECONDS)
        .checkValue(_ >= 0L, "must not be negative")
        .createWithDefaultString(default)
    }

  private def copyEnvironment(environment: JMap[String, String], target: SparkConf): Unit = {
    if (environment == null) return
    UserEntries.foreach { configEntry =>
      val raw = environment.get(environmentName(configEntry.key))
      if (raw != null && raw.trim.nonEmpty) target.set(configEntry.key, raw.trim)
    }
  }

  private def copyKnown(
      source: Iterator[(String, String)],
      target: SparkConf,
      allowedKeys: Set[String]): Unit = {
    source.foreach { case (key, value) =>
      if (allowedKeys.contains(key) && value != null) target.set(key, value.trim)
    }
  }

  private def validateAndFreeze(conf: SparkConf): TelemetryConfig = {
    val strict = try {
      value(conf, STRICT)
    } catch {
      case NonFatal(invalid) =>
        if (rawStrict(conf)) throw invalid
        return disabled()
    }

    try {
      read(conf, ENABLED)
      read(conf, BATCH_MAX_SIZE)
      read(conf, BATCH_TIMEOUT)
      read(conf, EXPORT_TIMEOUT)
      read(conf, SHUTDOWN_FLUSH_TIMEOUT)
      read(conf, SERVICE_NAME)
      read(conf, SERVICE_NAMESPACE)
      read(conf, DEPLOYMENT_ENVIRONMENT)
      read(conf, CLUSTER)
    } catch {
      case NonFatal(invalid) =>
        if (strict) throw invalid
        return disabled()
    }

    validateSignal(conf, strict, LOGS_ENABLED,
      Seq(LOG_CAPTURE, LOGS_QUEUE_CAPACITY, LOG_MINIMUM_LEVEL))
    validateSignal(conf, strict, TRACES_ENABLED,
      Seq(TRACES_QUEUE_CAPACITY, TASK_SAMPLE_RATE, SLOW_TASK_THRESHOLD))

    validateEndpoint(conf, strict, "OTLP endpoint is not an http(s) URI",
      Seq(METRICS_ENABLED, LOGS_ENABLED, TRACES_ENABLED), ENDPOINT)
    freeze(conf)
  }

  private def validateSignal(
      conf: SparkConf,
      strict: Boolean,
      enabledEntry: ConfigEntry[Boolean],
      entries: Seq[ConfigEntry[_]]): Unit = {
    try {
      read(conf, enabledEntry)
      entries.foreach(read(conf, _))
    } catch {
      case NonFatal(invalid) =>
        if (strict) throw invalid
        conf.set(enabledEntry, false)
        entries.foreach(conf.remove)
    }
  }

  private def invalidateEndpoint(
      conf: SparkConf,
      strict: Boolean,
      message: String,
      affectedSignals: Seq[ConfigEntry[Boolean]],
      endpointEntry: ConfigEntry[String]): Unit = {
    if (strict) throw new IllegalArgumentException(message)
    affectedSignals.foreach(conf.set(_, false))
    conf.remove(endpointEntry)
  }

  private def validateEndpoint(
      conf: SparkConf,
      strict: Boolean,
      message: String,
      affectedSignals: Seq[ConfigEntry[Boolean]],
      endpointEntry: ConfigEntry[String]): Unit = {
    try {
      if (!isHttpEndpoint(value(conf, endpointEntry))) throw new IllegalArgumentException(message)
    } catch {
      case NonFatal(invalid) =>
        if (strict) throw invalid
        invalidateEndpoint(conf, strict = false, message, affectedSignals, endpointEntry)
    }
  }

  private def read(conf: SparkConf, entry: ConfigEntry[_]): Unit = {
    value(conf, entry.asInstanceOf[ConfigEntry[Any]])
    ()
  }

  private def value[T](conf: SparkConf, entry: ConfigEntry[T]): T =
    entry.readFrom(literalReader(conf))

  private def literalReader(conf: SparkConf): ConfigReader = {
    val literalProvider = new ConfigProvider {
      override def get(key: String): Option[String] = conf.getOption(key).map { raw =>
        if (raw.contains("${")) {
          throw new IllegalArgumentException(key + " must not contain variable substitution")
        }
        raw
      }
    }
    val emptyProvider = new ConfigProvider {
      override def get(key: String): Option[String] = None
    }
    new ConfigReader(literalProvider).bindEnv(emptyProvider).bindSystem(emptyProvider)
  }

  private def freeze(source: SparkConf): TelemetryConfig = {
    val canonical = new SparkConf(false)
    val values = new LinkedHashMap[String, String]()
    val reader = literalReader(source)
    AllEntries.foreach { entry =>
      canonicalize(reader, canonical, values, entry.asInstanceOf[ConfigEntry[Any]])
    }
    new TelemetryConfig(canonical, Collections.unmodifiableMap(values))
  }

  private def canonicalize(
      reader: ConfigReader,
      target: SparkConf,
      values: JMap[String, String],
      entry: ConfigEntry[Any]): Unit = {
    val parsed = entry.readFrom(reader)
    val encoded = entry.stringConverter(parsed)
    target.set(entry.key, encoded)
    values.put(entry.key, encoded)
  }

  private def rawStrict(conf: SparkConf): Boolean =
    conf.getOption(STRICT.key).exists(_.trim.equalsIgnoreCase("true"))

  private def isHttpEndpoint(value: String): Boolean = {
    try {
      val uri = new URI(value)
      uri.getHost != null && uri.getUserInfo == null && uri.getQuery == null &&
        uri.getFragment == null &&
        (uri.getScheme.equalsIgnoreCase("http") || uri.getScheme.equalsIgnoreCase("https"))
    } catch {
      case _: Exception => false
    }
  }

  private def environmentName(key: String): String =
    key.toUpperCase(Locale.ROOT).replace('.', '_').replace('-', '_')

  private def trimTrailingSlash(value: String): String = value.reverse.dropWhile(_ == '/').reverse

  private def emptyTo(value: String, fallback: String): String =
    if (value == null || value.trim.isEmpty) fallback else value.trim
}
