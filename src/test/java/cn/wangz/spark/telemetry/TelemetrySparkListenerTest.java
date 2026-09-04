package cn.wangz.spark.telemetry;

import cn.wangz.spark.telemetry.runtime.DeferredTelemetrySink;
import cn.wangz.spark.telemetry.signal.traces.StageTaskMetrics;
import cn.wangz.spark.telemetry.signal.traces.TaskFailure;
import cn.wangz.spark.telemetry.signal.traces.TaskSpanHandle;
import cn.wangz.spark.telemetry.signal.traces.TraceSink;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.scheduler.TaskInfo;
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

    @Test
    void omitsTimelineWhenStageMetricsAreUnavailableAfterTaskEnd() {
        TaskMetrics taskMetrics = new TaskMetrics();
        taskMetrics.setExecutorRunTime(8L);
        RecordingTraceSink traces = new RecordingTraceSink();
        DeferredTelemetrySink sink = new DeferredTelemetrySink(4);
        sink.bind(traces);
        TelemetrySparkListener listener = new TelemetrySparkListener(sink);
        StageInfo stage = stageInfo(2, null);
        listener.onStageSubmitted(new SparkListenerStageSubmitted(stage, null));

        listener.onTaskEnd(new SparkListenerTaskEnd(
                2, 1, "ResultTask", null, taskInfo(0L, 10L, 0L), null, taskMetrics));
        listener.onStageCompleted(new SparkListenerStageCompleted(stage));

        assertEquals(1, traces.endedStages);
        assertNull(traces.stageTaskMetrics);
    }

    @Test
    void combinesStageAccumulatorsWithTaskDerivedTimelineMetrics() {
        TaskMetrics taskMetrics = new TaskMetrics();
        taskMetrics.setExecutorDeserializeTime(10L);
        taskMetrics.setExecutorRunTime(150L);
        taskMetrics.setResultSerializationTime(5L);
        taskMetrics.shuffleReadMetrics().incFetchWaitTime(30L);
        taskMetrics.shuffleWriteMetrics().incWriteTime(20_900_000L);
        TaskMetrics stageMetrics = new TaskMetrics();
        stageMetrics.setExecutorDeserializeTime(21L);
        stageMetrics.setResultSerializationTime(7L);
        stageMetrics.shuffleReadMetrics().incFetchWaitTime(40L);
        RecordingTraceSink traces = new RecordingTraceSink();
        DeferredTelemetrySink sink = new DeferredTelemetrySink(4);
        sink.bind(traces);
        TelemetrySparkListener listener = new TelemetrySparkListener(sink);
        StageInfo stage = stageInfo(2, stageMetrics);
        listener.onStageSubmitted(new SparkListenerStageSubmitted(stage, null));

        TaskInfo task = taskInfo(100L, 300L, 280L);
        listener.onTaskEnd(new SparkListenerTaskEnd(
                2, 1, "ResultTask", null, task, null, taskMetrics));
        listener.onTaskEnd(new SparkListenerTaskEnd(
                2, 1, "ResultTask", null, taskInfo(300L, 310L, 0L), null, null));
        listener.onStageCompleted(new SparkListenerStageCompleted(stage));

        StageTaskMetrics snapshot = traces.stageTaskMetrics;
        assertEquals(true, snapshot.timelineAvailable());
        assertEquals(15L, snapshot.schedulerDelayMillis());
        assertEquals(21L, snapshot.executorDeserializeTimeMillis());
        assertEquals(40L, snapshot.shuffleReadTimeMillis());
        assertEquals(100L, snapshot.executorComputingTimeMillis());
        assertEquals(20L, snapshot.shuffleWriteTimeMillis());
        assertEquals(7L, snapshot.resultSerializationTimeMillis());
        assertEquals(20L, snapshot.gettingResultTimeMillis());
        assertEquals(2L, snapshot.observedTaskAttempts());
        assertEquals(1L, snapshot.includedTaskAttempts());
    }

    @Test
    void isolatesTimelineMetricsByStageAttemptAndIgnoresLateTaskEvents() {
        TaskMetrics metrics = new TaskMetrics();
        metrics.setExecutorRunTime(8L);
        RecordingTraceSink traces = new RecordingTraceSink();
        DeferredTelemetrySink sink = new DeferredTelemetrySink(4);
        sink.bind(traces);
        TelemetrySparkListener listener = new TelemetrySparkListener(sink);
        StageInfo attemptZero = stageInfo(2, 0, metrics);
        StageInfo attemptOne = stageInfo(2, 1, metrics);
        listener.onStageSubmitted(new SparkListenerStageSubmitted(attemptZero, null));
        listener.onStageSubmitted(new SparkListenerStageSubmitted(attemptOne, null));

        listener.onTaskEnd(new SparkListenerTaskEnd(
                2, 1, "ResultTask", null, taskInfo(0L, 10L, 0L), null, metrics));
        listener.onStageCompleted(new SparkListenerStageCompleted(attemptZero));
        assertEquals(false, traces.stageTaskMetrics.timelineAvailable());

        listener.onTaskEnd(new SparkListenerTaskEnd(
                2, 0, "ResultTask", null, taskInfo(0L, 10L, 0L), null, metrics));
        listener.onStageCompleted(new SparkListenerStageCompleted(attemptOne));
        assertEquals(true, traces.stageTaskMetrics.timelineAvailable());
        assertEquals(2L, traces.stageTaskMetrics.schedulerDelayMillis());
        assertEquals(8L, traces.stageTaskMetrics.executorComputingTimeMillis());
    }

    @Test
    void omitsTimelineWhenTaskMetricsAreUnavailable() {
        TaskMetrics stageMetrics = new TaskMetrics();
        RecordingTraceSink traces = new RecordingTraceSink();
        DeferredTelemetrySink sink = new DeferredTelemetrySink(4);
        sink.bind(traces);
        TelemetrySparkListener listener = new TelemetrySparkListener(sink);
        StageInfo stage = stageInfo(2, stageMetrics);
        listener.onStageSubmitted(new SparkListenerStageSubmitted(stage, null));

        listener.onTaskEnd(new SparkListenerTaskEnd(
                2, 1, "ResultTask", null, taskInfo(0L, 10L, 0L), null, null));
        listener.onStageCompleted(new SparkListenerStageCompleted(stage));

        assertEquals(false, traces.stageTaskMetrics.timelineAvailable());
    }

    @Test
    void capsExecutorComputingAtSparkUiAdjustedRuntime() {
        TaskMetrics metrics = new TaskMetrics();
        metrics.setExecutorRunTime(150L);
        RecordingTraceSink traces = new RecordingTraceSink();
        DeferredTelemetrySink sink = new DeferredTelemetrySink(4);
        sink.bind(traces);
        TelemetrySparkListener listener = new TelemetrySparkListener(sink);
        StageInfo stage = stageInfo(2, metrics);
        listener.onStageSubmitted(new SparkListenerStageSubmitted(stage, null));

        listener.onTaskEnd(new SparkListenerTaskEnd(
                2, 1, "ResultTask", null, taskInfo(100L, 200L, 0L), null, metrics));
        listener.onStageCompleted(new SparkListenerStageCompleted(stage));

        assertEquals(0L, traces.stageTaskMetrics.schedulerDelayMillis());
        assertEquals(100L, traces.stageTaskMetrics.executorComputingTimeMillis());
    }

    @Test
    void floorsShuffleWriteMillisBeforeAggregatingTasks() {
        TaskMetrics metrics = new TaskMetrics();
        metrics.setExecutorRunTime(1L);
        metrics.shuffleWriteMetrics().incWriteTime(900_000L);
        RecordingTraceSink traces = new RecordingTraceSink();
        DeferredTelemetrySink sink = new DeferredTelemetrySink(4);
        sink.bind(traces);
        TelemetrySparkListener listener = new TelemetrySparkListener(sink);
        StageInfo stage = stageInfo(2, metrics);
        listener.onStageSubmitted(new SparkListenerStageSubmitted(stage, null));

        listener.onTaskEnd(new SparkListenerTaskEnd(
                2, 1, "ShuffleMapTask", null, taskInfo(100L, 101L, 0L), null, metrics));
        listener.onTaskEnd(new SparkListenerTaskEnd(
                2, 1, "ShuffleMapTask", null, taskInfo(200L, 201L, 0L), null, metrics));
        listener.onStageCompleted(new SparkListenerStageCompleted(stage));

        assertEquals(0L, traces.stageTaskMetrics.shuffleWriteTimeMillis());
        assertEquals(2L, traces.stageTaskMetrics.executorComputingTimeMillis());
        assertEquals(2L, traces.stageTaskMetrics.includedTaskAttempts());
    }

    @Test
    void clampsStageAccumulatorTimelineDurationsAtZero() {
        TaskMetrics taskMetrics = new TaskMetrics();
        taskMetrics.setExecutorRunTime(8L);
        TaskMetrics stageMetrics = new TaskMetrics();
        stageMetrics.setExecutorDeserializeTime(-10L);
        stageMetrics.setResultSerializationTime(-20L);
        stageMetrics.shuffleReadMetrics().incFetchWaitTime(-30L);
        RecordingTraceSink traces = new RecordingTraceSink();
        DeferredTelemetrySink sink = new DeferredTelemetrySink(4);
        sink.bind(traces);
        TelemetrySparkListener listener = new TelemetrySparkListener(sink);
        StageInfo stage = stageInfo(2, stageMetrics);
        listener.onStageSubmitted(new SparkListenerStageSubmitted(stage, null));

        listener.onTaskEnd(new SparkListenerTaskEnd(
                2, 1, "ResultTask", null, taskInfo(0L, 10L, 0L), null, taskMetrics));
        listener.onStageCompleted(new SparkListenerStageCompleted(stage));

        assertEquals(0L, traces.stageTaskMetrics.executorDeserializeTimeMillis());
        assertEquals(0L, traces.stageTaskMetrics.shuffleReadTimeMillis());
        assertEquals(0L, traces.stageTaskMetrics.resultSerializationTimeMillis());
    }

    private static TaskInfo taskInfo(long launchTime, long finishTime, long gettingResultTime) {
        TaskInfo task = new TaskInfo(
                1L, 0, 0, 0, launchTime, "driver", "localhost", null, false);
        task.finishTime_$eq(finishTime);
        task.gettingResultTime_$eq(gettingResultTime);
        return task;
    }

    private static StageInfo stageInfo(int stageId, TaskMetrics metrics) {
        return stageInfo(stageId, 1, metrics);
    }

    private static StageInfo stageInfo(int stageId, int attempt, TaskMetrics metrics) {
        return new StageInfo(
                stageId, attempt, "test stage", 1, null, null, "", metrics,
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
