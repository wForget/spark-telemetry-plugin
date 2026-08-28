package com.example.spark.telemetry.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Bounded bridge for the Driver lifecycle gap between DriverPlugin.init and registerMetrics.
 */
public final class DeferredTelemetrySink {
    private final Object lock = new Object();
    private final ArrayBlockingQueue<Event> bootstrapEvents;
    private volatile TelemetryRuntime runtime;
    private boolean closed;

    public DeferredTelemetrySink(int capacity) {
        bootstrapEvents = new ArrayBlockingQueue<Event>(Math.max(1, capacity));
    }

    public void bind(TelemetryRuntime telemetryRuntime) {
        List<Event> pending = new ArrayList<Event>();
        synchronized (lock) {
            if (closed || runtime != null) return;
            runtime = telemetryRuntime;
            bootstrapEvents.drainTo(pending);
        }
        for (Event event : pending) apply(event, telemetryRuntime);
    }

    public void close() {
        synchronized (lock) {
            closed = true;
            bootstrapEvents.clear();
            runtime = null;
        }
    }

    public void applicationEnded(final long time) {
        submit(new Event() { @Override public void apply(TelemetryRuntime runtime) { runtime.applicationEnded(time); } });
    }
    public void jobStarted(final int id, final int[] stages, final long time) {
        final int[] copied = stages.clone();
        submit(new Event() { @Override public void apply(TelemetryRuntime runtime) { runtime.jobStarted(id, copied, time); } });
    }
    public void jobEnded(
            final int id, final long end, final String outcome, final String failure) {
        submit(new Event() { @Override public void apply(TelemetryRuntime runtime) {
            runtime.jobEnded(id, end, outcome, failure);
        }});
    }
    public void stageStarted(final int id, final int attempt, final long time) {
        submit(new Event() { @Override public void apply(TelemetryRuntime runtime) { runtime.stageStarted(id, attempt, time); } });
    }
    public void stageEnded(
            final int id, final int attempt, final long end,
            final String outcome, final String failure) {
        submit(new Event() { @Override public void apply(TelemetryRuntime runtime) {
            runtime.stageEnded(id, attempt, end, outcome, failure);
        }});
    }

    private void submit(Event event) {
        TelemetryRuntime current;
        synchronized (lock) {
            if (closed) return;
            current = runtime;
            if (current == null) {
                bootstrapEvents.offer(event);
                return;
            }
        }
        apply(event, current);
    }

    private static void apply(Event event, TelemetryRuntime runtime) {
        try {
            event.apply(runtime);
        } catch (RuntimeException ignored) {
            // Spark listener delivery is fail-open.
        } catch (LinkageError ignored) {
            // Spark listener delivery is fail-open across provided-dependency ABI errors.
        }
    }

    private interface Event { void apply(TelemetryRuntime runtime); }
}
