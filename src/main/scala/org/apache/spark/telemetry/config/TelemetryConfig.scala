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
  def profilesEnabled(): Boolean = enabled() && conf.get(PROFILES_ENABLED)
  def logCaptureEnabled(): Boolean = logsEnabled() && conf.get(LOG_CAPTURE)
  def endpoint(): String = conf.get(ENDPOINT)
  def profileEndpoint(): String = conf.get(PROFILE_ENDPOINT)
  def profileEvent(): String = conf.get(PROFILE_EVENT)
  def profileInterval(): Duration = Duration.ofMillis(conf.get(PROFILE_INTERVAL))
  def profileUploadInterval(): Duration = Duration.ofMillis(conf.get(PROFILE_UPLOAD_INTERVAL))
  def profileJavaStackDepth(): Int = conf.get(PROFILE_JAVA_STACK_DEPTH)
  def profilesQueueCapacity(): Int = conf.get(PROFILES_QUEUE_CAPACITY)
  def profileAlloc(): String = conf.get(PROFILE_ALLOC)
  def profileLock(): String = conf.get(PROFILE_LOCK)
  def asyncProfilerExtraArguments(): String = conf.get(ASYNC_PROFILER_EXTRA_ARGUMENTS)
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
    validateProfileApplicationName(enriched, enriched.get(STRICT))
    freeze(enriched)
  }

  def toExecutorConfiguration(): JMap[String, String] = executorConfiguration
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
    text("endpoint", "Alloy OTLP gRPC base endpoint", "http://127.0.0.1:4317")

  val METRICS_ENABLED: ConfigEntry[Boolean] = signalEnabled("metrics")
  val LOGS_ENABLED: ConfigEntry[Boolean] = signalEnabled("logs")
  val TRACES_ENABLED: ConfigEntry[Boolean] = signalEnabled("traces")
  val PROFILES_ENABLED: ConfigEntry[Boolean] =
    boolean("profiles.enabled", "Enable Pyroscope continuous profiling", default = false)
  val PROFILE_ENDPOINT: ConfigEntry[String] =
    text("profile.endpoint", "Alloy Pyroscope HTTP endpoint", "http://127.0.0.1:9999")
  val PROFILE_EVENT: ConfigEntry[String] =
    define("profiles.event", "async-profiler primary sampling event") { builder =>
      builder.stringConf
        .transform(_.trim.toUpperCase(Locale.ROOT))
        .checkValues(Set("CPU", "WALL", "ITIMER"))
        .createWithDefault("ITIMER")
    }
  val PROFILE_INTERVAL: ConfigEntry[Long] =
    boundedTime("profiles.interval", "async-profiler sampling interval", "10ms", 5L, 1000L)
  val PROFILE_UPLOAD_INTERVAL: ConfigEntry[Long] =
    boundedTime("profiles.upload-interval", "Profile collection and upload interval",
      "10s", 2000L, 300000L)
  val PROFILE_JAVA_STACK_DEPTH: ConfigEntry[Int] =
    boundedInt("profiles.java-stack-depth", "Maximum Java stack depth", 2048, 64, 4096)
  val PROFILES_QUEUE_CAPACITY: ConfigEntry[Int] =
    boundedInt("queue.profiles.capacity", "Pyroscope upload queue capacity", 10, 1, 64)
  val PROFILE_ALLOC: ConfigEntry[String] =
    define("profiles.alloc", "Optional async-profiler allocation sampling interval") { builder =>
      builder.stringConf.transform(validateProfileAlloc).createWithDefault("")
    }
  val PROFILE_LOCK: ConfigEntry[String] =
    define("profiles.lock", "Optional async-profiler lock profiling threshold") { builder =>
      builder.stringConf.transform(validateProfileLock).createWithDefault("")
    }
  val ASYNC_PROFILER_EXTRA_ARGUMENTS: ConfigEntry[String] =
    define("profiles.async-profiler.extra-arguments", "Additional async-profiler arguments") { builder =>
      builder.stringConf.transform(validateAsyncProfilerArguments).createWithDefault("memlimit=128m")
    }

  val LOG_MINIMUM_LEVEL: ConfigEntry[String] =
    define("logs.minimum-level", "Minimum captured log level") { builder =>
      builder.stringConf
        .transform(_.trim.toUpperCase(Locale.ROOT))
        .checkValues(Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"))
        .createWithDefault("ERROR")
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
    METRICS_ENABLED, LOGS_ENABLED, TRACES_ENABLED, PROFILES_ENABLED,
    PROFILE_ENDPOINT, PROFILE_EVENT, PROFILE_INTERVAL, PROFILE_UPLOAD_INTERVAL,
    PROFILE_JAVA_STACK_DEPTH, PROFILES_QUEUE_CAPACITY, PROFILE_ALLOC, PROFILE_LOCK,
    ASYNC_PROFILER_EXTRA_ARGUMENTS,
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

  /** Safe fallback when configuration or a signal implementation cannot be read. */
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

  private def boundedInt(
      suffix: String,
      documentation: String,
      default: Int,
      minimum: Int,
      maximum: Int): ConfigEntry[Int] =
    define(suffix, documentation) { builder =>
      builder.intConf
        .checkValue(value => value >= minimum && value <= maximum,
          "must be between " + minimum + " and " + maximum)
        .createWithDefault(default)
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

  private def boundedTime(
      suffix: String,
      documentation: String,
      default: String,
      minimumMillis: Long,
      maximumMillis: Long): ConfigEntry[Long] =
    define(suffix, documentation) { builder =>
      builder.timeConf(TimeUnit.MILLISECONDS)
        .checkValue(value => value >= minimumMillis && value <= maximumMillis,
          "must be between " + minimumMillis + "ms and " + maximumMillis + "ms")
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
    validateSignal(conf, strict, PROFILES_ENABLED,
      Seq(PROFILE_EVENT, PROFILE_INTERVAL, PROFILE_UPLOAD_INTERVAL,
        PROFILE_JAVA_STACK_DEPTH, PROFILES_QUEUE_CAPACITY, PROFILE_ALLOC, PROFILE_LOCK,
        ASYNC_PROFILER_EXTRA_ARGUMENTS))

    validateEndpoint(conf, strict, "OTLP gRPC endpoint is not a safe base http(s) URI",
      Seq(METRICS_ENABLED, LOGS_ENABLED, TRACES_ENABLED), ENDPOINT)
    validateEndpoint(conf, strict, "Pyroscope endpoint is not a safe base http(s) URI",
      Seq(PROFILES_ENABLED), PROFILE_ENDPOINT)
    validateProfileApplicationName(conf, strict)
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

  private def validateEndpoint(
      conf: SparkConf,
      strict: Boolean,
      message: String,
      affectedSignals: Seq[ConfigEntry[Boolean]],
      endpointEntry: ConfigEntry[String]): Unit = {
    try {
      if (!isGrpcBaseEndpoint(value(conf, endpointEntry))) throw new IllegalArgumentException(message)
    } catch {
      case NonFatal(invalid) =>
        if (strict) throw invalid
        affectedSignals.foreach(conf.set(_, false))
        conf.remove(endpointEntry)
    }
  }

  private def validateProfileApplicationName(conf: SparkConf, strict: Boolean): Unit = {
    if (!value(conf, PROFILES_ENABLED)) return
    val name = value(conf, SERVICE_NAME)
    if (name.nonEmpty && !name.exists(character => character == '{' || character == '}')) return
    val invalid = new IllegalArgumentException(
      SERVICE_NAME.key + " must be non-empty and must not contain '{' or '}' when profiles are enabled")
    if (strict) throw invalid
    conf.set(PROFILES_ENABLED, false)
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

  private def isGrpcBaseEndpoint(value: String): Boolean = {
    try {
      val uri = new URI(value)
      val path = uri.getRawPath
      val port = uri.getPort
      uri.getHost != null && uri.getUserInfo == null && uri.getQuery == null &&
        uri.getFragment == null && (path == null || path.isEmpty || path == "/") &&
        (port == -1 || (port >= 1 && port <= 65535)) &&
        (uri.getScheme == "http" || uri.getScheme == "https")
    } catch {
      case _: Exception => false
    }
  }

  private val AllocationPattern = "^([1-9][0-9]*)([kKmMgG]?)$".r
  private val LockPattern = "^([1-9][0-9]*)(ns|us|ms|s)?$".r
  private val MemoryLimitPattern = "^([1-9][0-9]*)([mMgG])$".r

  private def validateProfileAlloc(raw: String): String = {
    val value = raw.trim
    if (value.isEmpty) return value
    val bytes = value match {
      case AllocationPattern(amount, unit) =>
        checkedMultiply(amount.toLong, unit.toLowerCase(Locale.ROOT) match {
          case "k" => 1024L
          case "m" => 1024L * 1024L
          case "g" => 1024L * 1024L * 1024L
          case _ => 1L
        }, PROFILE_ALLOC.key)
      case _ => throw new IllegalArgumentException(
        PROFILE_ALLOC.key + " must be empty or a byte count such as 512k")
    }
    if (bytes < 512L * 1024L || bytes > 1024L * 1024L * 1024L) {
      throw new IllegalArgumentException(PROFILE_ALLOC.key + " must be between 512k and 1g")
    }
    value
  }

  private def validateProfileLock(raw: String): String = {
    val value = raw.trim.toLowerCase(Locale.ROOT)
    if (value.isEmpty) return value
    val nanos = value match {
      case LockPattern(amount, unit) =>
        checkedMultiply(amount.toLong, Option(unit).getOrElse("ns") match {
          case "us" => 1000L
          case "ms" => 1000L * 1000L
          case "s" => 1000L * 1000L * 1000L
          case _ => 1L
        }, PROFILE_LOCK.key)
      case _ => throw new IllegalArgumentException(
        PROFILE_LOCK.key + " must be empty or a duration such as 10ms")
    }
    if (nanos < 1000L * 1000L || nanos > 60L * 1000L * 1000L * 1000L) {
      throw new IllegalArgumentException(PROFILE_LOCK.key + " must be between 1ms and 60s")
    }
    value
  }

  private def validateAsyncProfilerArguments(raw: String): String = {
    val arguments = raw.trim
    if (arguments.isEmpty) return "memlimit=128m"
    var seen = Set.empty[String]
    val normalized = Vector.newBuilder[String]
    arguments.split(",").iterator.map(_.trim).filter(_.nonEmpty).foreach { argument =>
      val separator = argument.indexOf('=')
      if (separator <= 0 || separator == argument.length - 1) {
        throw new IllegalArgumentException(
          ASYNC_PROFILER_EXTRA_ARGUMENTS.key + " only accepts name=value arguments")
      }
      val name = argument.substring(0, separator).trim.toLowerCase(Locale.ROOT)
      val value = argument.substring(separator + 1).trim.toLowerCase(Locale.ROOT)
      if (seen.contains(name)) {
        throw new IllegalArgumentException(
          ASYNC_PROFILER_EXTRA_ARGUMENTS.key + " contains duplicate argument '" + name + "'")
      }
      seen += name
      name match {
        case "cstack" if Set("fp", "dwarf", "vm", "vmx", "no").contains(value) => ()
        case "memlimit" => validateMemoryLimit(value)
        case "cstack" => throw new IllegalArgumentException(
          ASYNC_PROFILER_EXTRA_ARGUMENTS.key + " cstack must be fp, dwarf, vm, vmx, or no")
        case _ => throw new IllegalArgumentException(
          ASYNC_PROFILER_EXTRA_ARGUMENTS.key + " only allows cstack and memlimit")
      }
      normalized += name + "=" + value
    }
    if (!seen.contains("memlimit")) normalized += "memlimit=128m"
    normalized.result().mkString(",")
  }

  private def validateMemoryLimit(value: String): Unit = {
    val megabytes = value match {
      case MemoryLimitPattern(amount, unit) =>
        checkedMultiply(amount.toLong,
          if (unit.equalsIgnoreCase("g")) 1024L else 1L,
          ASYNC_PROFILER_EXTRA_ARGUMENTS.key)
      case _ => throw new IllegalArgumentException(
        ASYNC_PROFILER_EXTRA_ARGUMENTS.key + " memlimit must use m or g units")
    }
    if (megabytes < 16L || megabytes > 1024L) {
      throw new IllegalArgumentException(
        ASYNC_PROFILER_EXTRA_ARGUMENTS.key + " memlimit must be between 16m and 1g")
    }
  }

  private def checkedMultiply(value: Long, multiplier: Long, key: String): Long = {
    try Math.multiplyExact(value, multiplier) catch {
      case _: ArithmeticException => throw new IllegalArgumentException(key + " is too large")
    }
  }

  private def environmentName(key: String): String =
    key.toUpperCase(Locale.ROOT).replace('.', '_').replace('-', '_')

  private def emptyTo(value: String, fallback: String): String =
    if (value == null || value.trim.isEmpty) fallback else value.trim
}
