package cn.wangz.spark.telemetry

import java.util.{HashMap => JHashMap, Map => JMap}

import cn.wangz.spark.telemetry.runtime.{ResourceIdentity, TelemetryRuntime}
import cn.wangz.spark.telemetry.signal.logs.Log4j2TelemetryBridge
import cn.wangz.spark.telemetry.signal.metrics.SparkMetricRegistry
import cn.wangz.spark.telemetry.signal.profiles.ProfileScope
import cn.wangz.spark.telemetry.signal.traces.{TaskFailure, TaskSampler, TaskSpanHandle}
import org.apache.logging.log4j.ThreadContext
import org.apache.spark.SparkConf
import org.apache.spark.TaskContext
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.{ExceptionFailure, TaskFailedReason}
import org.apache.spark.telemetry.config.TelemetryConfig
import scala.util.control.NonFatal

final class TelemetryExecutorPlugin extends ExecutorPlugin {
  private val currentTask = new ThreadLocal[TaskState]()
  @volatile private var config: TelemetryConfig = _
  @volatile private var runtime: TelemetryRuntime = _
  @volatile private var logBridge: Log4j2TelemetryBridge = _
  @volatile private var sparkConf: SparkConf = _
  @volatile private var executorId: String = _

  override def init(context: PluginContext, extraConf: JMap[String, String]): Unit = {
    var strictRequested = false
    try {
      val conf = context.conf()
      sparkConf = conf
      executorId = context.executorID()
      strictRequested = TelemetryConfig.strictRequested(conf, false)
      strictRequested = TelemetryConfig.strictRequested(extraConf, strictRequested)
      val parsed = TelemetryConfig.fromDriver(extraConf).withApplication(
        conf.get("spark.app.name", "spark"),
        conf.get("spark.app.id", "unknown"))
      config = parsed
      if (parsed.enabled()) {
        val applicationId = conf.get("spark.app.id", parsed.applicationId())
        // In local mode Spark constructs the ExecutorPlugin before SparkContext assigns spark.app.id.
        // Defer runtime creation until the first task, by which point SparkContext has updated the
        // shared SparkConf. Otherwise every local task trace is permanently tagged app.id=unknown.
        if (applicationId != "unknown") {
          startRuntime(parsed, applicationId, context.executorID())
        }
      }
    } catch {
      case NonFatal(failure) =>
        if (strictRequested) throw failure
        config = TelemetryConfig.disabled()
      case failure: LinkageError =>
        if (strictRequested) throw failure
        config = null
    }
  }

  override def onTaskStart(): Unit = safely {
    val active = ensureRuntime()
    if (active != null && active.isRunning()) {
      val spark = TaskContext.get()
      if (spark != null) {
        val previous = new JHashMap[String, String]()
        val epochStartNanos = System.currentTimeMillis() * 1000000L
        val monotonicStartNanos = System.nanoTime()
        var span: TaskSpanHandle = null
        var profileScope: ProfileScope = ProfileScope.NONE
        var installed = false
        try {
          putContext(previous, "spark.stage.id", String.valueOf(spark.stageId()))
          putContext(previous, "spark.task.attempt.id", String.valueOf(spark.taskAttemptId()))
          span = active.traces().taskStarted(
            spark.taskAttemptId(), spark.stageId(), spark.stageAttemptNumber(), spark.partitionId(),
            spark.attemptNumber(), epochStartNanos)
          profileScope = active.openStageProfileScope(
            spark.stageId(), spark.stageAttemptNumber())
          if (span != null) {
            putContext(previous, "trace_id", span.traceId())
            putContext(previous, "span_id", span.spanId())
          }
          val state = new TaskState(
            spark.taskAttemptId(), monotonicStartNanos, epochStartNanos,
            span, profileScope, previous)
          currentTask.set(state)
          installed = true
        } finally {
          if (!installed) {
            try {
              profileScope.close()
            } finally {
              try {
                if (span != null) {
                  val duration = Math.max(0L, System.nanoTime() - monotonicStartNanos)
                  span.abandon(epochStartNanos + duration)
                }
              } finally {
                try restoreContext(previous)
                finally currentTask.remove()
              }
            }
          }
        }
      }
    }
  }

  override def onTaskSucceeded(): Unit = complete(failed = false, failure = null)

  override def onTaskFailed(reason: TaskFailedReason): Unit =
    complete(failed = true, safeFailureDetails(reason))

  override def shutdown(): Unit = {
    val bridge = logBridge
    logBridge = null
    if (bridge != null) safely(bridge.close())
    val active = runtime
    runtime = null
    if (active != null && config != null) safely(active.close(config.shutdownFlushTimeout()))
    currentTask.remove()
  }

  private def complete(failed: Boolean, failure: TaskFailure): Unit = safely {
    val state = currentTask.get()
    if (state != null) {
      currentTask.remove()
      val end = System.nanoTime()
      val duration = Math.max(0L, end - state.monotonicStartNanos)
      val endEpochNanos = state.epochStartNanos + duration
      try {
        state.profileScope.close()
      } finally {
        try {
          val traced = TaskSampler.shouldTrace(
            state.taskAttemptId,
            failed,
            duration,
            config.slowTaskThreshold().toNanos,
            config.taskSampleRate())
          val slow = duration >= config.slowTaskThreshold().toNanos
          val active = runtime
          if (active != null) {
            active.traces().taskEnded(
              state.span, endEpochNanos,
              if (failed) "failure" else "success", failure, traced, slow)
          }
        } finally {
          try {
            if (state.span != null) state.span.abandon(endEpochNanos)
          } finally {
            restoreContext(state.previousContext)
          }
        }
      }
    }
  }

  private def failureDetails(reason: TaskFailedReason): TaskFailure = {
    if (reason == null) {
      new TaskFailure(
        "UnknownReason", "org.apache.spark.UnknownReason", "unknown",
        true, null, "", "", "")
    } else reason match {
      case exceptionFailure: ExceptionFailure =>
        new TaskFailure(
          failureType(exceptionFailure),
          failureClass(exceptionFailure),
          exceptionFailure.description,
          exceptionFailure.countTowardsTaskFailures,
          exceptionFailure.exception.orNull,
          exceptionFailure.className,
          exceptionFailure.description,
          Option(exceptionFailure.fullStackTrace)
            .filter(_.nonEmpty)
            .getOrElse(exceptionFailure.toErrorString))
      case other =>
        new TaskFailure(
          failureType(other),
          failureClass(other),
          other.toErrorString,
          other.countTowardsTaskFailures,
          null, "", "", "")
    }
  }

  private def safeFailureDetails(reason: TaskFailedReason): TaskFailure = {
    try failureDetails(reason) catch {
      case NonFatal(_) => unknownFailure
      case _: LinkageError => unknownFailure
    }
  }

  private def unknownFailure: TaskFailure =
    new TaskFailure(
      "UnknownReason", "org.apache.spark.TaskFailedReason", "task failed",
      true, null, "", "", "")

  private def failureType(reason: TaskFailedReason): String =
    reason.getClass.getSimpleName.stripSuffix("$")

  private def failureClass(reason: TaskFailedReason): String =
    reason.getClass.getName.stripSuffix("$")

  private def putContext(previous: JHashMap[String, String], key: String, value: String): Unit = {
    previous.put(key, ThreadContext.get(key))
    ThreadContext.put(key, value)
  }

  private def restoreContext(previous: JHashMap[String, String]): Unit = {
    val iterator = previous.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      if (entry.getValue == null) ThreadContext.remove(entry.getKey)
      else ThreadContext.put(entry.getKey, entry.getValue)
    }
  }

  private def safely(action: => Unit): Unit = {
    try action catch {
      case NonFatal(_) => ()
      case _: LinkageError => ()
    }
  }

  private def ensureRuntime(): TelemetryRuntime = {
    var active = runtime
    if (active == null && config != null && config.enabled()) this.synchronized {
      active = runtime
      if (active == null) {
        val applicationId = sparkConf.get("spark.app.id", config.applicationId())
        if (applicationId != "unknown") {
          startRuntime(config, applicationId, executorId)
          active = runtime
        }
      }
    }
    active
  }

  private def startRuntime(
      parsed: TelemetryConfig,
      applicationId: String,
      currentExecutorId: String): Unit = {
    val finalConfig = parsed.withApplication(parsed.applicationName(), applicationId)
    config = finalConfig
    val isLocalExecutor = currentExecutorId == "driver"
    val identity = ResourceIdentity.executor(finalConfig, applicationId, currentExecutorId)
    // Local mode shares the Driver's SparkEnv and MetricsSystem. The Driver runtime already
    // exports that registry, so a second reader here would duplicate every metric.
    val sparkMetrics =
      if (finalConfig.metricsEnabled() && !isLocalExecutor)
        SparkMetricRegistry.current()
      else
        null
    // A local-mode ExecutorPlugin shares the Driver JVM. The Driver exclusively owns the
    // process-wide Pyroscope agent, so this runtime must not start or stop a second instance.
    val created = TelemetryRuntime.create(finalConfig, identity, sparkMetrics, !isLocalExecutor)
    runtime = created
    // Local mode also shares the Driver's Log4j context. The Driver bridge captures task logs
    // after onTaskStart installs trace/span context, so a second root appender would duplicate them.
    if (finalConfig.logCaptureEnabled() && !isLocalExecutor) {
      logBridge = Log4j2TelemetryBridge.install("executor-" + currentExecutorId, created.logs())
    }
  }

  private final class TaskState(
      val taskAttemptId: Long,
      val monotonicStartNanos: Long,
      val epochStartNanos: Long,
      val span: TaskSpanHandle,
      val profileScope: ProfileScope,
      val previousContext: JHashMap[String, String])
}
