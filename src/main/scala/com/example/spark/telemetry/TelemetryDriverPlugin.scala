package com.example.spark.telemetry

import java.util.{Collections, Map => JMap}

import com.example.spark.telemetry.runtime.{DeferredTelemetrySink, ResourceIdentity, TelemetryRuntime}
import com.example.spark.telemetry.signal.logs.Log4j2TelemetryBridge
import com.example.spark.telemetry.signal.metrics.SparkMetricRegistry
import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import org.apache.spark.telemetry.config.TelemetryConfig
import scala.util.control.NonFatal

final class TelemetryDriverPlugin extends DriverPlugin {
  @volatile private var config: TelemetryConfig = _
  @volatile private var runtime: TelemetryRuntime = _
  @volatile private var logBridge: Log4j2TelemetryBridge = _
  @volatile private var deferred: DeferredTelemetrySink = _
  private var applicationStartMillis: Long = 0L

  override def init(sc: SparkContext, context: PluginContext): JMap[String, String] = {
    var strictRequested = false
    try {
      strictRequested = TelemetryConfig.strictRequested(sc.getConf, false)
      val parsed = TelemetryConfig.from(sc.getConf)
        .withApplication(sc.appName, "unknown")
      config = parsed
      applicationStartMillis = sc.startTime
      deferred = new DeferredTelemetrySink(parsed.tracesQueueCapacity())
      sc.addSparkListener(new TelemetrySparkListener(deferred))
      parsed.toExecutorConfiguration()
    } catch {
      case NonFatal(failure) =>
        if (strictRequested) throw failure
        config = TelemetryConfig.disabled()
        config.toExecutorConfiguration()
      case failure: LinkageError =>
        if (strictRequested) throw failure
        config = null
        Collections.singletonMap("spark.telemetry.enabled", "false")
    }
  }

  override def registerMetrics(applicationId: String, context: PluginContext): Unit = {
    if (config == null || !config.enabled()) return
    try {
      val finalConfig = config.withApplication(config.applicationName(), applicationId)
      config = finalConfig
      val sparkMetrics = if (finalConfig.metricsEnabled()) SparkMetricRegistry.current() else null
      val created = TelemetryRuntime.create(
        finalConfig, ResourceIdentity.driver(finalConfig, applicationId), sparkMetrics)
      created.applicationStarted(applicationStartMillis)
      runtime = created
      deferred.bind(created)
      if (finalConfig.logCaptureEnabled()) {
        logBridge = Log4j2TelemetryBridge.install("driver-" + applicationId, created.logs())
      }
    } catch {
      case NonFatal(failure) =>
        if (config.strict()) throw failure
      case failure: LinkageError =>
        if (config.strict()) throw failure
    }
  }

  override def shutdown(): Unit = {
    val bridge = logBridge
    logBridge = null
    if (bridge != null) quietly(bridge.close())
    val sink = deferred
    deferred = null
    if (sink != null) sink.close()
    val active = runtime
    runtime = null
    if (active != null) quietly(active.close(config.shutdownFlushTimeout()))
  }

  private def quietly(action: => Unit): Unit = {
    try action catch {
      case NonFatal(_) => ()
      case _: LinkageError => ()
    }
  }
}
