package cn.wangz.spark.telemetry.signal.traces;

/** Immutable snapshot of Spark task metrics accumulated for one stage attempt. */
public final class StageTaskMetrics {
    private final long executorRunTimeMillis;
    private final long memoryBytesSpilled;
    private final long diskBytesSpilled;
    private final long inputBytesRead;
    private final long outputBytesWritten;
    private final long shuffleReadBytes;
    private final long shuffleFetchWaitTimeMillis;
    private final long shuffleWriteBytes;
    private final long shuffleWriteTimeNanos;
    private final boolean timelineAvailable;
    private final long schedulerDelayMillis;
    private final long executorDeserializeTimeMillis;
    private final long shuffleReadTimeMillis;
    private final long executorComputingTimeMillis;
    private final long shuffleWriteTimeMillis;
    private final long resultSerializationTimeMillis;
    private final long gettingResultTimeMillis;
    private final long observedTaskAttempts;
    private final long includedTaskAttempts;

    public StageTaskMetrics(
            long executorRunTimeMillis,
            long memoryBytesSpilled,
            long diskBytesSpilled,
            long inputBytesRead,
            long outputBytesWritten,
            long shuffleReadBytes,
            long shuffleFetchWaitTimeMillis,
            long shuffleWriteBytes,
            long shuffleWriteTimeNanos) {
        this(
                executorRunTimeMillis,
                memoryBytesSpilled,
                diskBytesSpilled,
                inputBytesRead,
                outputBytesWritten,
                shuffleReadBytes,
                shuffleFetchWaitTimeMillis,
                shuffleWriteBytes,
                shuffleWriteTimeNanos,
                false,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L,
                0L);
    }

    public StageTaskMetrics(
            long executorRunTimeMillis,
            long memoryBytesSpilled,
            long diskBytesSpilled,
            long inputBytesRead,
            long outputBytesWritten,
            long shuffleReadBytes,
            long shuffleFetchWaitTimeMillis,
            long shuffleWriteBytes,
            long shuffleWriteTimeNanos,
            long schedulerDelayMillis,
            long executorDeserializeTimeMillis,
            long shuffleReadTimeMillis,
            long executorComputingTimeMillis,
            long shuffleWriteTimeMillis,
            long resultSerializationTimeMillis,
            long gettingResultTimeMillis,
            long observedTaskAttempts,
            long includedTaskAttempts) {
        this(
                executorRunTimeMillis,
                memoryBytesSpilled,
                diskBytesSpilled,
                inputBytesRead,
                outputBytesWritten,
                shuffleReadBytes,
                shuffleFetchWaitTimeMillis,
                shuffleWriteBytes,
                shuffleWriteTimeNanos,
                includedTaskAttempts > 0L,
                schedulerDelayMillis,
                executorDeserializeTimeMillis,
                shuffleReadTimeMillis,
                executorComputingTimeMillis,
                shuffleWriteTimeMillis,
                resultSerializationTimeMillis,
                gettingResultTimeMillis,
                observedTaskAttempts,
                includedTaskAttempts);
    }

    private StageTaskMetrics(
            long executorRunTimeMillis,
            long memoryBytesSpilled,
            long diskBytesSpilled,
            long inputBytesRead,
            long outputBytesWritten,
            long shuffleReadBytes,
            long shuffleFetchWaitTimeMillis,
            long shuffleWriteBytes,
            long shuffleWriteTimeNanos,
            boolean timelineAvailable,
            long schedulerDelayMillis,
            long executorDeserializeTimeMillis,
            long shuffleReadTimeMillis,
            long executorComputingTimeMillis,
            long shuffleWriteTimeMillis,
            long resultSerializationTimeMillis,
            long gettingResultTimeMillis,
            long observedTaskAttempts,
            long includedTaskAttempts) {
        this.executorRunTimeMillis = executorRunTimeMillis;
        this.memoryBytesSpilled = memoryBytesSpilled;
        this.diskBytesSpilled = diskBytesSpilled;
        this.inputBytesRead = inputBytesRead;
        this.outputBytesWritten = outputBytesWritten;
        this.shuffleReadBytes = shuffleReadBytes;
        this.shuffleFetchWaitTimeMillis = shuffleFetchWaitTimeMillis;
        this.shuffleWriteBytes = shuffleWriteBytes;
        this.shuffleWriteTimeNanos = shuffleWriteTimeNanos;
        this.timelineAvailable = timelineAvailable;
        this.schedulerDelayMillis = schedulerDelayMillis;
        this.executorDeserializeTimeMillis = executorDeserializeTimeMillis;
        this.shuffleReadTimeMillis = shuffleReadTimeMillis;
        this.executorComputingTimeMillis = executorComputingTimeMillis;
        this.shuffleWriteTimeMillis = shuffleWriteTimeMillis;
        this.resultSerializationTimeMillis = resultSerializationTimeMillis;
        this.gettingResultTimeMillis = gettingResultTimeMillis;
        this.observedTaskAttempts = observedTaskAttempts;
        this.includedTaskAttempts = includedTaskAttempts;
    }

    public long executorRunTimeMillis() { return executorRunTimeMillis; }

    public long memoryBytesSpilled() { return memoryBytesSpilled; }

    public long diskBytesSpilled() { return diskBytesSpilled; }

    public long inputBytesRead() { return inputBytesRead; }

    public long outputBytesWritten() { return outputBytesWritten; }

    public long shuffleReadBytes() { return shuffleReadBytes; }

    public long shuffleFetchWaitTimeMillis() { return shuffleFetchWaitTimeMillis; }

    public long shuffleWriteBytes() { return shuffleWriteBytes; }

    public long shuffleWriteTimeNanos() { return shuffleWriteTimeNanos; }

    public boolean timelineAvailable() { return timelineAvailable; }

    public long schedulerDelayMillis() { return schedulerDelayMillis; }

    public long executorDeserializeTimeMillis() { return executorDeserializeTimeMillis; }

    public long shuffleReadTimeMillis() { return shuffleReadTimeMillis; }

    public long executorComputingTimeMillis() { return executorComputingTimeMillis; }

    public long shuffleWriteTimeMillis() { return shuffleWriteTimeMillis; }

    public long resultSerializationTimeMillis() { return resultSerializationTimeMillis; }

    public long gettingResultTimeMillis() { return gettingResultTimeMillis; }

    public long observedTaskAttempts() { return observedTaskAttempts; }

    public long includedTaskAttempts() { return includedTaskAttempts; }
}
