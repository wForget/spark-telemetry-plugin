package com.example.spark.telemetry

import java.util.{HashMap => JHashMap, Map => JMap}

import com.example.spark.telemetry.runtime.{ResourceIdentity, TelemetryRuntime}
import com.example.spark.telemetry.signal.logs.Log4j2TelemetryBridge
import com.example.spark.telemetry.signal.traces.{TaskSampler, TaskSpanHandle}
import org.apache.logging.log4j.ThreadContext
import org.apache.spark.TaskContext
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.apache.spark.TaskFailedReason
import org.apache.spark.telemetry.config.TelemetryConfig
import scala.util.control.NonFatal

final class TelemetryExecutorPlugin extends ExecutorPlugin {
  private val currentTask = new ThreadLocal[TaskState]()
  @volatile private var config: TelemetryConfig = _
  @volatile private var runtime: TelemetryRuntime = _
  @volatile private var logBridge: Log4j2TelemetryBridge = _

  override def init(context: PluginContext, extraConf: JMap[String, String]): Unit = {
    var strictRequested = false
    try {
      val conf = context.conf()
      strictRequested = TelemetryConfig.strictRequested(conf, false)
      strictRequested = TelemetryConfig.strictRequested(extraConf, strictRequested)
      val parsed = TelemetryConfig.fromDriver(extraConf).withApplication(
        conf.get("spark.app.name", "spark"),
        conf.get("spark.app.id", "unknown"))
      config = parsed
      if (parsed.enabled()) {
        val identity = ResourceIdentity.executor(
          parsed,
          conf.get("spark.app.id", "unknown"),
          context.executorID())
        val created = TelemetryRuntime.create(parsed, identity)
        runtime = created
        if (parsed.logCaptureEnabled()) {
          logBridge = Log4j2TelemetryBridge.install("executor-" + context.executorID(), created.logs())
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
    val active = runtime
    if (active != null && active.isRunning()) {
      val spark = TaskContext.get()
      if (spark != null) {
        val previous = new JHashMap[String, String]()
        putContext(previous, "spark.stage.id", String.valueOf(spark.stageId()))
        putContext(previous, "spark.task.attempt.id", String.valueOf(spark.taskAttemptId()))
        val epochStartNanos = System.currentTimeMillis() * 1000000L
        val span = active.taskTraceStarted(
          spark.taskAttemptId(), spark.stageId(), spark.stageAttemptNumber(), spark.partitionId(),
          spark.attemptNumber(), epochStartNanos)
        if (span != null) {
          putContext(previous, "trace_id", span.traceId())
          putContext(previous, "span_id", span.spanId())
        }
        val state = new TaskState(
          spark.taskAttemptId(), spark.stageId(), spark.stageAttemptNumber(), spark.partitionId(),
          spark.attemptNumber(), System.nanoTime(), epochStartNanos, span, previous)
        currentTask.set(state)
      }
    }
  }

  override def onTaskSucceeded(): Unit = complete(failed = false, "")

  override def onTaskFailed(reason: TaskFailedReason): Unit =
    complete(failed = true, if (reason == null) "unknown" else reason.toString)

  override def shutdown(): Unit = {
    val bridge = logBridge
    logBridge = null
    if (bridge != null) quietly(bridge.close())
    val active = runtime
    runtime = null
    if (active != null && config != null) quietly(active.close(config.shutdownFlushTimeout()))
    currentTask.remove()
  }

  private def complete(failed: Boolean, failure: String): Unit = safely {
    val state = currentTask.get()
    if (state != null) {
      currentTask.remove()
      try {
        val end = System.nanoTime()
        val duration = Math.max(0L, end - state.monotonicStartNanos)
        val traced = TaskSampler.shouldTrace(
          state.taskAttemptId,
          failed,
          duration,
          config.slowTaskThreshold().toNanos,
          config.taskSampleRate())
        val active = runtime
        if (active != null) {
          active.taskTraceEnded(
            state.span, state.epochStartNanos + duration,
            if (failed) "failure" else "success", failure, traced)
        } else if (state.span != null) {
          state.span.abandon(state.epochStartNanos + duration)
        }
      } finally {
        restoreContext(state.previousContext)
      }
    }
  }

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
  private def quietly(action: => Unit): Unit = safely(action)

  private final class TaskState(
      val taskAttemptId: Long,
      val stageId: Int,
      val stageAttempt: Int,
      val partitionId: Int,
      val attemptNumber: Int,
      val monotonicStartNanos: Long,
      val epochStartNanos: Long,
      val span: TaskSpanHandle,
      val previousContext: JHashMap[String, String])
}
