package com.example.spark.telemetry.reliability;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** A small, thread-safe closed/open/half-open circuit breaker. */
public final class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    @FunctionalInterface
    public interface NanoClock {
        long nanoTime();
    }

    /** A single-use, generation-bound request permission. */
    public static final class Permit {
        private final CircuitBreaker owner;
        private final long generation;
        private final boolean probe;
        private final AtomicBoolean completed = new AtomicBoolean();

        private Permit(CircuitBreaker owner, long generation, boolean probe) {
            this.owner = owner;
            this.generation = generation;
            this.probe = probe;
        }

        public void recordSuccess() {
            if (completed.compareAndSet(false, true)) {
                owner.record(this, true);
            }
        }

        public void recordFailure() {
            if (completed.compareAndSet(false, true)) {
                owner.record(this, false);
            }
        }
    }

    private final int failureThreshold;
    private final long openDurationNanos;
    private final NanoClock clock;
    private final ThreadLocal<Permit> currentPermit = new ThreadLocal<Permit>();
    private State state = State.CLOSED;
    private int consecutiveFailures;
    private long openedAtNanos;
    private long generation;
    private boolean probeInFlight;

    public CircuitBreaker(int failureThreshold, long openDuration, TimeUnit unit) {
        this(failureThreshold, openDuration, unit, System::nanoTime);
    }

    public CircuitBreaker(int failureThreshold, long openDuration, TimeUnit unit, NanoClock clock) {
        if (failureThreshold <= 0) {
            throw new IllegalArgumentException("failureThreshold must be greater than zero");
        }
        Objects.requireNonNull(unit, "unit");
        long durationNanos = unit.toNanos(openDuration);
        if (openDuration <= 0L || durationNanos <= 0L) {
            throw new IllegalArgumentException("openDuration must be greater than zero");
        }
        this.failureThreshold = failureThreshold;
        this.openDurationNanos = durationNanos;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Returns a permit or {@code null} while requests are blocked. */
    public synchronized Permit tryAcquirePermit() {
        if (state == State.CLOSED) {
            return new Permit(this, generation, false);
        }
        if (state == State.OPEN) {
            if (clock.nanoTime() - openedAtNanos < openDurationNanos) {
                return null;
            }
            state = State.HALF_OPEN;
            generation++;
            probeInFlight = true;
            return new Permit(this, generation, true);
        }
        if (!probeInFlight) {
            probeInFlight = true;
            return new Permit(this, generation, true);
        }
        return null;
    }

    /**
     * Compatibility API for synchronous call sites. Cross-thread attempts must
     * retain and complete the object returned by {@link #tryAcquirePermit()}.
     */
    public boolean tryAcquire() {
        Permit permit = tryAcquirePermit();
        if (permit == null) {
            currentPermit.remove();
            return false;
        }
        currentPermit.set(permit);
        return true;
    }

    public void recordSuccess() {
        completeCurrent(true);
    }

    public void recordFailure() {
        completeCurrent(false);
    }

    public synchronized State state() {
        return state;
    }

    public synchronized int consecutiveFailures() {
        return consecutiveFailures;
    }

    private void completeCurrent(boolean success) {
        Permit permit = currentPermit.get();
        currentPermit.remove();
        if (permit == null) {
            return;
        }
        if (success) {
            permit.recordSuccess();
        } else {
            permit.recordFailure();
        }
    }

    private synchronized void record(Permit permit, boolean success) {
        if (permit.generation != generation) {
            return;
        }
        if (permit.probe) {
            if (state != State.HALF_OPEN || !probeInFlight) {
                return;
            }
            if (success) {
                closeCircuit();
            } else {
                openCircuit();
            }
        } else if (state == State.CLOSED) {
            if (success) {
                consecutiveFailures = 0;
            } else if (++consecutiveFailures >= failureThreshold) {
                openCircuit();
            }
        }
    }

    private void openCircuit() {
        state = State.OPEN;
        openedAtNanos = clock.nanoTime();
        consecutiveFailures = failureThreshold;
        probeInFlight = false;
        generation++;
    }

    private void closeCircuit() {
        state = State.CLOSED;
        consecutiveFailures = 0;
        probeInFlight = false;
        generation++;
    }
}
