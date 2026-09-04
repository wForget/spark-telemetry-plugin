package cn.wangz.spark.telemetry.signal.traces;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Driver span lifecycle plus independent, sampled task traces. */
public final class TracePipeline implements TraceSink {
    private static final AttributeKey<Long> SPARK_JOB_ID = AttributeKey.longKey("spark.job.id");
    private static final AttributeKey<Long> SPARK_STAGE_ID = AttributeKey.longKey("spark.stage.id");
    private static final AttributeKey<Long> SPARK_STAGE_TASK_EXECUTOR_RUN_TIME =
            AttributeKey.longKey("spark.stage.task_metrics.executor_run_time_ms");
    private static final AttributeKey<Long> SPARK_STAGE_TASK_MEMORY_BYTES_SPILLED =
            AttributeKey.longKey("spark.stage.task_metrics.memory_bytes_spilled");
    private static final AttributeKey<Long> SPARK_STAGE_TASK_DISK_BYTES_SPILLED =
            AttributeKey.longKey("spark.stage.task_metrics.disk_bytes_spilled");
    private static final AttributeKey<Long> SPARK_STAGE_TASK_INPUT_BYTES_READ =
            AttributeKey.longKey("spark.stage.task_metrics.input.bytes_read");
    private static final AttributeKey<Long> SPARK_STAGE_TASK_OUTPUT_BYTES_WRITTEN =
            AttributeKey.longKey("spark.stage.task_metrics.output.bytes_written");
    private static final AttributeKey<Long> SPARK_STAGE_TASK_SHUFFLE_READ_BYTES =
            AttributeKey.longKey("spark.stage.task_metrics.shuffle.read.total_bytes_read");
    private static final AttributeKey<Long> SPARK_STAGE_TASK_SHUFFLE_FETCH_WAIT_TIME =
            AttributeKey.longKey("spark.stage.task_metrics.shuffle.read.fetch_wait_time_ms");
    private static final AttributeKey<Long> SPARK_STAGE_TASK_SHUFFLE_WRITE_BYTES =
            AttributeKey.longKey("spark.stage.task_metrics.shuffle.write.bytes_written");
    private static final AttributeKey<Long> SPARK_STAGE_TASK_SHUFFLE_WRITE_TIME =
            AttributeKey.longKey("spark.stage.task_metrics.shuffle.write.write_time_ns");
    private static final AttributeKey<Long> SPARK_TASK_ATTEMPT_ID =
            AttributeKey.longKey("spark.task.attempt.id");

    private final Tracer tracer;
    private final Map<Integer, Span> jobs = new HashMap<Integer, Span>();
    private final Map<String, Span> stages = new HashMap<String, Span>();
    private final Map<Integer, Set<Integer>> stageJobs = new HashMap<Integer, Set<Integer>>();
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private boolean accepting;
    private Span application;

    public TracePipeline(Tracer tracer, boolean enabled) {
        this.tracer = tracer;
        this.accepting = enabled;
    }

    @Override
    public void applicationStarted(final long epochMillis) {
        runDriver(() -> {
            if (application != null) return;
            application = tracer.spanBuilder("spark.application")
                    .setNoParent()
                    .setStartTimestamp(epochMillis, TimeUnit.MILLISECONDS)
                    .startSpan();
        });
    }

    @Override
    public void applicationEnded(final long epochMillis) {
        runDriver(() -> applicationEndedUnsafe(epochMillis));
    }

    @Override
    public void jobStarted(final int jobId, final int[] stageIds, final long epochMillis) {
        runDriver(() -> {
            if (jobs.containsKey(jobId)) return;
            SpanBuilder builder = tracer.spanBuilder("spark.job")
                    .setAttribute(SPARK_JOB_ID, (long) jobId)
                    .setStartTimestamp(epochMillis, TimeUnit.MILLISECONDS);
            Span app = application;
            builder = app == null
                    ? builder.setNoParent()
                    : builder.setParent(Context.root().with(app));
            jobs.put(jobId, builder.startSpan());
            if (stageIds == null) return;
            for (int stageId : stageIds) {
                Set<Integer> owners = stageJobs.get(stageId);
                if (owners == null) {
                    owners = new HashSet<Integer>();
                    stageJobs.put(stageId, owners);
                }
                owners.add(jobId);
            }
        });
    }

    @Override
    public void jobEnded(
            final int jobId,
            final long epochMillis,
            final String outcome,
            final String failure) {
        runDriver(() -> {
            Span span = jobs.remove(jobId);
            if (span != null) endSafely(span, epochMillis, outcome, failure);
            Iterator<Map.Entry<Integer, Set<Integer>>> entries = stageJobs.entrySet().iterator();
            while (entries.hasNext()) {
                Set<Integer> owners = entries.next().getValue();
                owners.remove(jobId);
                if (owners.isEmpty()) entries.remove();
            }
        });
    }

    @Override
    public void stageStarted(final int stageId, final int attempt, final long epochMillis) {
        runDriver(() -> {
            String key = stageKey(stageId, attempt);
            if (stages.containsKey(key)) return;
            SpanBuilder builder = tracer.spanBuilder("spark.stage")
                    .setAttribute(SPARK_STAGE_ID, (long) stageId)
                    .setAttribute("spark.stage.attempt", (long) attempt)
                    .setStartTimestamp(epochMillis, TimeUnit.MILLISECONDS);
            Span app = application;
            builder = app == null
                    ? builder.setNoParent()
                    : builder.setParent(Context.root().with(app));
            Set<Integer> owners = stageJobs.get(stageId);
            if (owners != null) {
                for (Integer jobId : owners) {
                    Span job = jobs.get(jobId);
                    if (job != null && job.getSpanContext().isValid()) {
                        builder.addLink(job.getSpanContext());
                    }
                }
            }
            stages.put(key, builder.startSpan());
        });
    }

    @Override
    public void stageEnded(
            int stageId,
            int attempt,
            long epochMillis,
            String outcome,
            String failure) {
        stageEnded(stageId, attempt, epochMillis, outcome, failure, null);
    }

    @Override
    public void stageEnded(
            final int stageId,
            final int attempt,
            final long epochMillis,
            final String outcome,
            final String failure,
            final StageTaskMetrics taskMetrics) {
        runDriver(() -> {
            Span span = stages.remove(stageKey(stageId, attempt));
            if (span != null) endStageSafely(
                    span, epochMillis, outcome, failure, taskMetrics);
        });
    }

    @Override
    public TaskSpanHandle taskStarted(
            long taskAttemptId,
            int stageId,
            int stageAttempt,
            int partitionId,
            int attemptNumber,
            long startEpochNanos) {
        Lock operation = lifecycleLock.readLock();
        operation.lock();
        Span span = null;
        Scope scope = null;
        try {
            if (!accepting) return null;
            span = tracer.spanBuilder("spark.task")
                    .setNoParent()
                    .setAttribute(SPARK_TASK_ATTEMPT_ID, taskAttemptId)
                    .setAttribute(SPARK_STAGE_ID, (long) stageId)
                    .setAttribute("spark.stage.attempt", (long) stageAttempt)
                    .setAttribute("spark.task.partition", (long) partitionId)
                    .setAttribute("spark.task.attempt-number", (long) attemptNumber)
                    .setStartTimestamp(startEpochNanos, TimeUnit.NANOSECONDS)
                    .startSpan();
            scope = span.makeCurrent();
            TaskSpanHandle handle = new TaskSpanHandle(span, scope);
            span = null;
            scope = null;
            return handle;
        } catch (RuntimeException | LinkageError ignored) {
            discardStartedTask(span, scope, startEpochNanos);
            return null;
        } finally {
            operation.unlock();
        }
    }

    @Override
    public void taskEnded(
            TaskSpanHandle handle,
            long endEpochNanos,
            String outcome,
            TaskFailure failure,
            boolean retain,
            boolean slow) {
        if (handle == null) return;
        Lock operation = lifecycleLock.readLock();
        operation.lock();
        try {
            runSafely(() -> {
                if (accepting) handle.end(endEpochNanos, outcome, failure, retain, slow);
                else handle.abandon(endEpochNanos);
            });
        } finally {
            operation.unlock();
        }
    }

    public void close(final long epochMillis) {
        Lock close = lifecycleLock.writeLock();
        close.lock();
        try {
            if (!accepting) return;
            accepting = false;
            runSafely(() -> applicationEndedUnsafe(epochMillis));
        } finally {
            close.unlock();
        }
    }

    private void runDriver(Runnable action) {
        Lock operation = lifecycleLock.writeLock();
        operation.lock();
        try {
            if (!accepting) return;
            runSafely(action);
        } finally {
            operation.unlock();
        }
    }

    private void applicationEndedUnsafe(long epochMillis) {
        endOrphans(epochMillis);
        Span span = application;
        application = null;
        if (span != null) endApplicationSafely(span, epochMillis);
    }

    private void endOrphans(long epochMillis) {
        try {
            for (Span stage : stages.values()) {
                endSafely(stage, epochMillis, "cancelled", "application stopped");
            }
        } finally {
            stages.clear();
        }
        try {
            for (Span job : jobs.values()) {
                endSafely(job, epochMillis, "cancelled", "application stopped");
            }
        } finally {
            jobs.clear();
            stageJobs.clear();
        }
    }

    private static void discardStartedTask(Span span, Scope scope, long epochNanos) {
        if (scope != null) runSafely(() -> scope.close());
        if (span != null) {
            runSafely(() -> span.setAttribute(TaskFilteringSpanProcessor.RETAIN_TASK, false));
            runSafely(() -> span.end(epochNanos, TimeUnit.NANOSECONDS));
        }
    }

    private static void endApplicationSafely(Span span, long epochMillis) {
        runSafely(() -> span.end(epochMillis, TimeUnit.MILLISECONDS));
    }

    private static void endStageSafely(
            Span span,
            long epochMillis,
            String outcome,
            String failure,
            StageTaskMetrics taskMetrics) {
        if (taskMetrics != null) {
            runSafely(() -> {
                span.setAttribute(SPARK_STAGE_TASK_EXECUTOR_RUN_TIME,
                        taskMetrics.executorRunTimeMillis());
                span.setAttribute(SPARK_STAGE_TASK_MEMORY_BYTES_SPILLED,
                        taskMetrics.memoryBytesSpilled());
                span.setAttribute(SPARK_STAGE_TASK_DISK_BYTES_SPILLED,
                        taskMetrics.diskBytesSpilled());
                span.setAttribute(SPARK_STAGE_TASK_INPUT_BYTES_READ,
                        taskMetrics.inputBytesRead());
                span.setAttribute(SPARK_STAGE_TASK_OUTPUT_BYTES_WRITTEN,
                        taskMetrics.outputBytesWritten());
                span.setAttribute(SPARK_STAGE_TASK_SHUFFLE_READ_BYTES,
                        taskMetrics.shuffleReadBytes());
                span.setAttribute(SPARK_STAGE_TASK_SHUFFLE_FETCH_WAIT_TIME,
                        taskMetrics.shuffleFetchWaitTimeMillis());
                span.setAttribute(SPARK_STAGE_TASK_SHUFFLE_WRITE_BYTES,
                        taskMetrics.shuffleWriteBytes());
                span.setAttribute(SPARK_STAGE_TASK_SHUFFLE_WRITE_TIME,
                        taskMetrics.shuffleWriteTimeNanos());
            });
        }
        endSafely(span, epochMillis, outcome, failure);
    }

    private static void endSafely(
            Span span, long epochMillis, String outcome, String failure) {
        runSafely(() -> {
            span.setAttribute("outcome", outcome);
            if (!"success".equals(outcome)) span.setStatus(StatusCode.ERROR, safe(failure));
        });
        runSafely(() -> span.end(epochMillis, TimeUnit.MILLISECONDS));
    }

    private static String safe(String value) { return value == null ? "" : value; }
    private static String stageKey(int stageId, int attempt) { return stageId + ":" + attempt; }

    private static void runSafely(Runnable action) {
        try {
            action.run();
        } catch (RuntimeException | LinkageError ignored) {
            // Telemetry is fail-open by contract.
        }
    }
}
