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
        this.executorRunTimeMillis = executorRunTimeMillis;
        this.memoryBytesSpilled = memoryBytesSpilled;
        this.diskBytesSpilled = diskBytesSpilled;
        this.inputBytesRead = inputBytesRead;
        this.outputBytesWritten = outputBytesWritten;
        this.shuffleReadBytes = shuffleReadBytes;
        this.shuffleFetchWaitTimeMillis = shuffleFetchWaitTimeMillis;
        this.shuffleWriteBytes = shuffleWriteBytes;
        this.shuffleWriteTimeNanos = shuffleWriteTimeNanos;
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
}
