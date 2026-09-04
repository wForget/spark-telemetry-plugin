package cn.wangz.spark.telemetry;

import cn.wangz.spark.telemetry.runtime.DeferredTelemetrySink;
import cn.wangz.spark.telemetry.signal.traces.StageTaskMetrics;
import cn.wangz.spark.telemetry.signal.traces.TaskFailure;
import cn.wangz.spark.telemetry.signal.traces.TaskSpanHandle;
import cn.wangz.spark.telemetry.signal.traces.TraceSink;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TelemetrySparkListenerTest {
    @Test
    void snapshotsAccumulatedStageTaskMetrics() {
        TaskMetrics metrics = new TaskMetrics();
        metrics.setExecutorRunTime(101L);
        metrics.incMemoryBytesSpilled(102L);
        metrics.incDiskBytesSpilled(103L);
        metrics.inputMetrics().incBytesRead(104L);
        metrics.outputMetrics().setBytesWritten(105L);
        metrics.shuffleReadMetrics().incRemoteBytesRead(60L);
        metrics.shuffleReadMetrics().incLocalBytesRead(46L);
        metrics.shuffleReadMetrics().incFetchWaitTime(107L);
        metrics.shuffleWriteMetrics().incBytesWritten(108L);
        metrics.shuffleWriteMetrics().incWriteTime(109L);
        RecordingTraceSink traces = new RecordingTraceSink();
        DeferredTelemetrySink sink = new DeferredTelemetrySink(4);
        sink.bind(traces);

        new TelemetrySparkListener(sink).onStageCompleted(
                new SparkListenerStageCompleted(stageInfo(2, metrics)));

        StageTaskMetrics snapshot = traces.stageTaskMetrics;
        assertEquals(101L, snapshot.executorRunTimeMillis());
        assertEquals(102L, snapshot.memoryBytesSpilled());
        assertEquals(103L, snapshot.diskBytesSpilled());
        assertEquals(104L, snapshot.inputBytesRead());
        assertEquals(105L, snapshot.outputBytesWritten());
        assertEquals(106L, snapshot.shuffleReadBytes());
        assertEquals(107L, snapshot.shuffleFetchWaitTimeMillis());
        assertEquals(108L, snapshot.shuffleWriteBytes());
        assertEquals(109L, snapshot.shuffleWriteTimeNanos());
    }

    @Test
    void completesStageWhenTaskMetricsAreUnavailable() {
        RecordingTraceSink traces = new RecordingTraceSink();
        DeferredTelemetrySink sink = new DeferredTelemetrySink(4);
        sink.bind(traces);

        new TelemetrySparkListener(sink).onStageCompleted(
                new SparkListenerStageCompleted(stageInfo(2, null)));

        assertEquals(1, traces.endedStages);
        assertNull(traces.stageTaskMetrics);
    }

    private static StageInfo stageInfo(int stageId, TaskMetrics metrics) {
        return new StageInfo(
                stageId, 1, "test stage", 1, null, null, "", metrics,
                null, null, 0, false, 0);
    }

    private static final class RecordingTraceSink implements TraceSink {
        private int endedStages;
        private StageTaskMetrics stageTaskMetrics;

        @Override public void applicationStarted(long epochMillis) {}

        @Override public void applicationEnded(long epochMillis) {}

        @Override public void jobStarted(int jobId, int[] stageIds, long epochMillis) {}

        @Override public void jobEnded(
                int jobId, long epochMillis, String outcome, String failure) {}

        @Override public void stageStarted(int stageId, int attempt, long epochMillis) {}

        @Override public void stageEnded(
                int stageId,
                int attempt,
                long epochMillis,
                String outcome,
                String failure) {
            stageEnded(stageId, attempt, epochMillis, outcome, failure, null);
        }

        @Override public void stageEnded(
                int stageId,
                int attempt,
                long epochMillis,
                String outcome,
                String failure,
                StageTaskMetrics taskMetrics) {
            endedStages++;
            stageTaskMetrics = taskMetrics;
        }

        @Override public TaskSpanHandle taskStarted(
                long taskAttemptId,
                int stageId,
                int stageAttempt,
                int partitionId,
                int attemptNumber,
                long startEpochNanos) {
            return null;
        }

        @Override public void taskEnded(
                TaskSpanHandle handle,
                long endEpochNanos,
                String outcome,
                TaskFailure failure,
                boolean retain,
                boolean slow) {}
    }
}
