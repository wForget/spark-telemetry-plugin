package cn.wangz.spark.telemetry

import cn.wangz.spark.telemetry.runtime.DeferredTelemetrySink
import org.apache.spark.scheduler._

private[telemetry] final class TelemetrySparkListener(sink: DeferredTelemetrySink)
    extends SparkListener {
  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit =
    sink.applicationEnded(event.time)

  override def onJobStart(event: SparkListenerJobStart): Unit = {
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
    if (event.jobResult == JobSucceeded)
      sink.jobEnded(event.jobId, event.time, "success", "")
    else
      sink.jobEnded(event.jobId, event.time, "failure", String.valueOf(event.jobResult))
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val info = event.stageInfo
    val start = info.submissionTime.getOrElse(System.currentTimeMillis())
    sink.stageStarted(info.stageId, info.attemptNumber(), start)
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val info = event.stageInfo
    val end = info.completionTime.getOrElse(System.currentTimeMillis())
    info.failureReason match {
      case Some(failure) =>
        sink.stageEnded(info.stageId, info.attemptNumber(), end, "failure", failure)
      case None =>
        sink.stageEnded(info.stageId, info.attemptNumber(), end, "success", "")
    }
  }
}
