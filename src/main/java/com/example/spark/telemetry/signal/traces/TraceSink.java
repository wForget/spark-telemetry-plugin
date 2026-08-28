package com.example.spark.telemetry.signal.traces;

/**
 * Lifecycle-safe trace operations exposed to Spark adapters.
 *
 * <p>Implementations are fail-open: telemetry failures never escape into Spark callbacks.
 */
public interface TraceSink {
    void applicationStarted(long epochMillis);

    void applicationEnded(long epochMillis);

    void jobStarted(int jobId, int[] stageIds, long epochMillis);

    void jobEnded(int jobId, long epochMillis, String outcome, String failure);

    void stageStarted(int stageId, int attempt, long epochMillis);

    void stageEnded(int stageId, int attempt, long epochMillis, String outcome, String failure);

    TaskSpanHandle taskStarted(
            long taskAttemptId,
            int stageId,
            int stageAttempt,
            int partitionId,
            int attemptNumber,
            long startEpochNanos);

    void taskEnded(
            TaskSpanHandle handle,
            long endEpochNanos,
            String outcome,
            String failure,
            boolean retain);
}
