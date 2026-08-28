package com.example.spark.telemetry.signal.traces;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** Driver span lifecycle plus independent, sampled task traces. */
public final class TracePipeline {
    private static final AttributeKey<Long> SPARK_JOB_ID = AttributeKey.longKey("spark.job.id");
    private static final AttributeKey<Long> SPARK_STAGE_ID = AttributeKey.longKey("spark.stage.id");
    private static final AttributeKey<Long> SPARK_TASK_ATTEMPT_ID = AttributeKey.longKey("spark.task.attempt.id");
    private final Tracer tracer;
    private final Map<Integer, Span> jobs = new ConcurrentHashMap<Integer, Span>();
    private final Map<String, Span> stages = new ConcurrentHashMap<String, Span>();
    private final Map<Integer, Set<Integer>> stageJobs = new ConcurrentHashMap<Integer, Set<Integer>>();
    private volatile Span application;

    public TracePipeline(Tracer tracer) {
        this.tracer = tracer;
    }

    public void applicationStarted(long epochMillis) {
        application = tracer.spanBuilder("spark.application")
                .setNoParent()
                .setStartTimestamp(epochMillis, TimeUnit.MILLISECONDS)
                .startSpan();
    }

    public void applicationEnded(long epochMillis) {
        Span span = application;
        if (span != null) {
            span.end(epochMillis, TimeUnit.MILLISECONDS);
            application = null;
        }
        endOrphans(epochMillis);
    }

    public void jobStarted(int jobId, int[] stageIds, long epochMillis) {
        SpanBuilder builder = tracer.spanBuilder("spark.job")
                .setAttribute(SPARK_JOB_ID, (long) jobId)
                .setStartTimestamp(epochMillis, TimeUnit.MILLISECONDS);
        Span app = application;
        builder = app == null ? builder.setNoParent() : builder.setParent(Context.root().with(app));
        jobs.put(jobId, builder.startSpan());
        for (int stageId : stageIds) {
            Set<Integer> owners = stageJobs.get(stageId);
            if (owners == null) {
                Set<Integer> created = ConcurrentHashMap.newKeySet();
                owners = stageJobs.putIfAbsent(stageId, created);
                if (owners == null) owners = created;
            }
            owners.add(jobId);
        }
    }

    public void jobEnded(int jobId, long epochMillis, String outcome, String failure) {
        Span span = jobs.remove(jobId);
        if (span != null) end(span, epochMillis, outcome, failure);
        for (Map.Entry<Integer, Set<Integer>> entry : stageJobs.entrySet()) {
            Set<Integer> owners = entry.getValue();
            owners.remove(jobId);
            if (owners.isEmpty()) stageJobs.remove(entry.getKey(), owners);
        }
    }

    public void stageStarted(int stageId, int attempt, long epochMillis) {
        SpanBuilder builder = tracer.spanBuilder("spark.stage")
                .setAttribute(SPARK_STAGE_ID, (long) stageId)
                .setAttribute("spark.stage.attempt", (long) attempt)
                .setStartTimestamp(epochMillis, TimeUnit.MILLISECONDS);
        Span app = application;
        builder = app == null ? builder.setNoParent() : builder.setParent(Context.root().with(app));
        Set<Integer> owners = stageJobs.get(stageId);
        if (owners != null) {
            for (Integer jobId : owners) {
                Span job = jobs.get(jobId);
                if (job != null && job.getSpanContext().isValid()) builder.addLink(job.getSpanContext());
            }
        }
        stages.put(stageKey(stageId, attempt), builder.startSpan());
    }

    public void stageEnded(int stageId, int attempt, long epochMillis, String outcome, String failure) {
        Span span = stages.remove(stageKey(stageId, attempt));
        if (span != null) end(span, epochMillis, outcome, failure);
    }

    public TaskSpanHandle taskStarted(
            long taskAttemptId,
            int stageId,
            int stageAttempt,
            int partitionId,
            int attemptNumber,
            long startEpochNanos) {
        Span span = tracer.spanBuilder("spark.task")
                .setNoParent()
                .setAttribute(SPARK_TASK_ATTEMPT_ID, taskAttemptId)
                .setAttribute(SPARK_STAGE_ID, (long) stageId)
                .setAttribute("spark.stage.attempt", (long) stageAttempt)
                .setAttribute("spark.task.partition", (long) partitionId)
                .setAttribute("spark.task.attempt-number", (long) attemptNumber)
                .setStartTimestamp(startEpochNanos, TimeUnit.NANOSECONDS)
                .startSpan();
        return new TaskSpanHandle(span, span.makeCurrent());
    }

    public void taskEnded(
            TaskSpanHandle handle,
            long endEpochNanos,
            String outcome,
            String failure,
            boolean retain) {
        if (handle != null) handle.end(endEpochNanos, outcome, failure, retain);
    }

    public void close(long epochMillis) {
        applicationEnded(epochMillis);
    }

    private void endOrphans(long epochMillis) {
        for (Span stage : stages.values()) end(stage, epochMillis, "cancelled", "application stopped");
        stages.clear();
        for (Span job : jobs.values()) end(job, epochMillis, "cancelled", "application stopped");
        jobs.clear();
        stageJobs.clear();
    }

    private static void end(Span span, long epochMillis, String outcome, String failure) {
        span.setAttribute("outcome", outcome);
        if (!"success".equals(outcome)) span.setStatus(StatusCode.ERROR, safe(failure));
        span.end(epochMillis, TimeUnit.MILLISECONDS);
    }

    private static String safe(String value) { return value == null ? "" : value; }
    private static String stageKey(int stageId, int attempt) { return stageId + ":" + attempt; }
}
