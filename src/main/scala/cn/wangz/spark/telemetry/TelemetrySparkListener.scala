package cn.wangz.spark.telemetry

import cn.wangz.spark.telemetry.runtime.DeferredTelemetrySink
import cn.wangz.spark.telemetry.signal.traces.StageTaskMetrics
import org.apache.spark.scheduler._

import scala.collection.mutable

private[telemetry] final class TelemetrySparkListener(sink: DeferredTelemetrySink)
    extends SparkListener {
  private val stageTimelineMetrics = mutable.HashMap.empty[String, StageTimelineMetrics]

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    stageTimelineMetrics.clear()
    sink.applicationEnded(event.time)
  }

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
    stageTimelineMetrics.put(
      stageKey(info.stageId, info.attemptNumber()), new StageTimelineMetrics)
    sink.stageStarted(info.stageId, info.attemptNumber(), start)
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val info = event.stageInfo
    val end = info.completionTime.getOrElse(System.currentTimeMillis())
    val timeline = stageTimelineMetrics.remove(stageKey(info.stageId, info.attemptNumber()))
    val metrics = snapshotStageTaskMetrics(info, timeline)
    info.failureReason match {
      case Some(failure) =>
        sink.stageEnded(info.stageId, info.attemptNumber(), end, "failure", failure, metrics)
      case None =>
        sink.stageEnded(info.stageId, info.attemptNumber(), end, "success", "", metrics)
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    try {
      val metrics = event.taskMetrics
      val info = event.taskInfo
      val timeline = stageTimelineMetrics.get(stageKey(event.stageId, event.stageAttemptId))
      if (info != null && info.finished) timeline.foreach(_.recordObservedTask())
      if (metrics != null && info != null && info.finished) {
        val duration = nonNegative(info.duration)
        val executorRunTime = nonNegative(metrics.executorRunTime)
        val executorDeserializeTime = nonNegative(metrics.executorDeserializeTime)
        val resultSerializationTime = nonNegative(metrics.resultSerializationTime)
        val gettingResultTime =
          if (info.gettingResultTime > 0L)
            nonNegative(info.finishTime - info.gettingResultTime)
          else 0L
        val schedulerDelay = nonNegative(
          duration - executorRunTime - executorDeserializeTime -
            resultSerializationTime - gettingResultTime)
        val shuffleReadTime = nonNegative(metrics.shuffleReadMetrics.fetchWaitTime)
        val shuffleWriteTime = nonNegative(metrics.shuffleWriteMetrics.writeTime / 1000000L)
        val adjustedExecutorRunTime = nonNegative(
          duration - executorDeserializeTime - resultSerializationTime -
            gettingResultTime - schedulerDelay)
        val executorComputingTime = nonNegative(
          adjustedExecutorRunTime - shuffleReadTime - shuffleWriteTime)
        timeline.foreach(_.add(
            schedulerDelay,
            executorComputingTime,
            shuffleWriteTime,
            gettingResultTime))
      }
    } catch {
      case _: RuntimeException | _: LinkageError =>
    }
  }

  private def snapshotStageTaskMetrics(
      info: StageInfo,
      timeline: Option[StageTimelineMetrics]): StageTaskMetrics = {
    try {
      val metrics = info.taskMetrics
      if (metrics == null) null
      else timeline.filter(_.taskCount > 0L) match {
        case Some(values) => new StageTaskMetrics(
          metrics.executorRunTime,
          metrics.memoryBytesSpilled,
          metrics.diskBytesSpilled,
          metrics.inputMetrics.bytesRead,
          metrics.outputMetrics.bytesWritten,
          metrics.shuffleReadMetrics.totalBytesRead,
          metrics.shuffleReadMetrics.fetchWaitTime,
          metrics.shuffleWriteMetrics.bytesWritten,
          metrics.shuffleWriteMetrics.writeTime,
          values.schedulerDelayMillis,
          nonNegative(metrics.executorDeserializeTime),
          nonNegative(metrics.shuffleReadMetrics.fetchWaitTime),
          values.executorComputingTimeMillis,
          values.shuffleWriteTimeMillis,
          nonNegative(metrics.resultSerializationTime),
          values.gettingResultTimeMillis,
          values.observedTaskAttempts,
          values.includedTaskAttempts)
        case None => new StageTaskMetrics(
          metrics.executorRunTime,
          metrics.memoryBytesSpilled,
          metrics.diskBytesSpilled,
          metrics.inputMetrics.bytesRead,
          metrics.outputMetrics.bytesWritten,
          metrics.shuffleReadMetrics.totalBytesRead,
          metrics.shuffleReadMetrics.fetchWaitTime,
          metrics.shuffleWriteMetrics.bytesWritten,
          metrics.shuffleWriteMetrics.writeTime)
      }
    } catch {
      case _: RuntimeException | _: LinkageError => null
    }
  }

  private def stageKey(stageId: Int, attempt: Int): String = s"$stageId:$attempt"

  private def nonNegative(value: Long): Long = math.max(value, 0L)

  private final class StageTimelineMetrics {
    var observedTaskAttempts = 0L
    var includedTaskAttempts = 0L
    var schedulerDelayMillis = 0L
    var executorComputingTimeMillis = 0L
    var shuffleWriteTimeMillis = 0L
    var gettingResultTimeMillis = 0L

    def taskCount: Long = includedTaskAttempts

    def recordObservedTask(): Unit = observedTaskAttempts += 1L

    def add(
        schedulerDelay: Long,
        executorComputingTime: Long,
        shuffleWriteTime: Long,
        gettingResultTime: Long): Unit = {
      includedTaskAttempts += 1L
      schedulerDelayMillis += schedulerDelay
      executorComputingTimeMillis += executorComputingTime
      shuffleWriteTimeMillis += shuffleWriteTime
      gettingResultTimeMillis += gettingResultTime
    }
  }
}
