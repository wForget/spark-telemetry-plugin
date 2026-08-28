package com.example.spark.telemetry.signal.traces;

/** Stable task-id sampler: failures and slow tasks are always retained. */
public final class TaskSampler {
    private TaskSampler() {
    }

    public static boolean shouldTrace(
            long taskAttemptId,
            boolean failed,
            long durationNanos,
            long slowThresholdNanos,
            double normalSampleRate) {
        if (failed || durationNanos >= slowThresholdNanos) return true;
        if (normalSampleRate <= 0.0d) return false;
        if (normalSampleRate >= 1.0d) return true;
        long mixed = mix64(taskAttemptId);
        double unit = (double) (mixed >>> 11) * 0x1.0p-53;
        return unit < normalSampleRate;
    }

    private static long mix64(long value) {
        long z = value + 0x9e3779b97f4a7c15L;
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }
}
