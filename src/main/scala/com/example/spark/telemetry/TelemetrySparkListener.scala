package com.example.spark.telemetry

import java.util.concurrent.ConcurrentHashMap

import com.example.spark.telemetry.runtime.DeferredTelemetrySink
import org.apache.spark.scheduler._

private[telemetry] final class TelemetrySparkListener(sink: DeferredTelemetrySink)
    extends SparkListener {
  private val jobStarts = new ConcurrentHashMap[Integer, java.lang.Long]()
  private val stageStarts = new ConcurrentHashMap[String, java.lang.Long]()

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit =
    sink.applicationEnded(event.time)

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    jobStarts.put(Integer.valueOf(event.jobId), java.lang.Long.valueOf(event.time))
    val ids = new Array[Int](event.stageIds.size)
    val iterator = event.stageIds.iterator
    var index = 0
    while (iterator.hasNext) {
      ids(index) = iterator.next()
      index += 1
    }
    sink.jobStarted(event.jobId, ids, event.time)
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    val start = valueOr(jobStarts.remove(Integer.valueOf(event.jobId)), event.time)
    if (event.jobResult == JobSucceeded)
      sink.jobEnded(event.jobId, start, event.time, "success", "")
    else
      sink.jobEnded(event.jobId, start, event.time, "failure", String.valueOf(event.jobResult))
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val info = event.stageInfo
    val start = info.submissionTime.getOrElse(System.currentTimeMillis())
    stageStarts.put(stageKey(info.stageId, info.attemptNumber()), java.lang.Long.valueOf(start))
    sink.stageStarted(info.stageId, info.attemptNumber(), start)
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val info = event.stageInfo
    val end = info.completionTime.getOrElse(System.currentTimeMillis())
    val start = valueOr(stageStarts.remove(stageKey(info.stageId, info.attemptNumber())), end)
    info.failureReason match {
      case Some(failure) =>
        sink.stageEnded(info.stageId, info.attemptNumber(), start, end, "failure", failure)
      case None =>
        sink.stageEnded(info.stageId, info.attemptNumber(), start, end, "success", "")
    }
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = sink.executorAdded()
  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = sink.executorRemoved()
  override def onTaskStart(event: SparkListenerTaskStart): Unit = sink.taskStarted()
  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val outcome = if (String.valueOf(event.reason) == "Success") "success" else "failure"
    sink.taskEnded(Math.max(0L, event.taskInfo.duration), outcome)
  }

  private def stageKey(stageId: Int, attempt: Int): String = stageId.toString + ":" + attempt
  private def valueOr(value: java.lang.Long, fallback: Long): Long =
    if (value == null) fallback else value.longValue()
}
