package com.example.spark.telemetry.reliability;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/** Immutable exponential-backoff policy with bounded proportional jitter. */
public final class RetryPolicy {

    @FunctionalInterface
    public interface JitterSource {
        /** Returns a value in the half-open range {@code [0, 1)}. */
        double nextDouble();
    }

    private final long initialDelayMillis;
    private final long maxDelayMillis;
    private final double multiplier;
    private final double jitterRatio;
    private final int maxRetries;
    private final long maxElapsedMillis;
    private final JitterSource jitterSource;

    public RetryPolicy(
            long initialDelayMillis,
            long maxDelayMillis,
            double multiplier,
            double jitterRatio,
            int maxRetries,
            long maxElapsedMillis,
            JitterSource jitterSource) {
        if (initialDelayMillis < 0L) {
            throw new IllegalArgumentException("initialDelayMillis must not be negative");
        }
        if (maxDelayMillis < initialDelayMillis) {
            throw new IllegalArgumentException("maxDelayMillis must be >= initialDelayMillis");
        }
        if (!Double.isFinite(multiplier) || multiplier < 1.0d) {
            throw new IllegalArgumentException("multiplier must be finite and >= 1");
        }
        if (!Double.isFinite(jitterRatio) || jitterRatio < 0.0d || jitterRatio > 1.0d) {
            throw new IllegalArgumentException("jitterRatio must be between 0 and 1");
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must not be negative");
        }
        if (maxElapsedMillis < 0L) {
            throw new IllegalArgumentException("maxElapsedMillis must not be negative");
        }
        this.initialDelayMillis = initialDelayMillis;
        this.maxDelayMillis = maxDelayMillis;
        this.multiplier = multiplier;
        this.jitterRatio = jitterRatio;
        this.maxRetries = maxRetries;
        this.maxElapsedMillis = maxElapsedMillis;
        this.jitterSource = Objects.requireNonNull(jitterSource, "jitterSource");
    }

    public RetryPolicy(
            long initialDelayMillis,
            long maxDelayMillis,
            double multiplier,
            double jitterRatio,
            int maxRetries,
            long maxElapsedMillis) {
        this(
                initialDelayMillis,
                maxDelayMillis,
                multiplier,
                jitterRatio,
                maxRetries,
                maxElapsedMillis,
                () -> ThreadLocalRandom.current().nextDouble());
    }

    /**
     * Returns the delay for a one-based retry attempt. Attempt {@code 1} uses
     * the initial delay. Jitter is symmetric around the exponential base.
     */
    public long delayMillis(int retryAttempt) {
        if (retryAttempt <= 0) {
            throw new IllegalArgumentException("retryAttempt must be one-based");
        }
        double exponential = initialDelayMillis * Math.pow(multiplier, retryAttempt - 1.0d);
        long baseDelay = exponential >= maxDelayMillis || !Double.isFinite(exponential)
                ? maxDelayMillis
                : (long) exponential;
        if (baseDelay == 0L || jitterRatio == 0.0d) {
            return baseDelay;
        }

        double random = jitterSource.nextDouble();
        if (!Double.isFinite(random) || random < 0.0d || random >= 1.0d) {
            throw new IllegalStateException("jitter source must return a value in [0, 1)");
        }
        double jittered = baseDelay * (1.0d - jitterRatio + (2.0d * jitterRatio * random));
        return Math.min(maxDelayMillis, Math.max(0L, Math.round(jittered)));
    }

    /**
     * Determines whether a one-based retry attempt fits both retry limits. The
     * caller remains responsible for classifying an error as retryable.
     */
    public boolean shouldRetry(int retryAttempt, long elapsedMillis) {
        return retryAttempt > 0
                && retryAttempt <= maxRetries
                && elapsedMillis >= 0L
                && elapsedMillis < maxElapsedMillis;
    }

    /**
     * Returns a jittered delay only when it fully fits the remaining elapsed-time
     * budget, or {@code -1} otherwise. This method consumes the
     * jitter source exactly once and is the preferred scheduling API.
     */
    public long nextDelayMillis(int retryAttempt, long elapsedMillis) {
        if (!shouldRetry(retryAttempt, elapsedMillis)) {
            return -1L;
        }
        long delay = delayMillis(retryAttempt);
        return delay <= maxElapsedMillis - elapsedMillis ? delay : -1L;
    }

    public int maxRetries() {
        return maxRetries;
    }

    public long maxElapsedMillis() {
        return maxElapsedMillis;
    }
}
