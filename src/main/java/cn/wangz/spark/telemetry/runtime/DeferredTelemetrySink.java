package cn.wangz.spark.telemetry.runtime;

import cn.wangz.spark.telemetry.signal.traces.TraceSink;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Bounded bridge for the Driver lifecycle gap between DriverPlugin.init and registerMetrics.
 */
public final class DeferredTelemetrySink {
    private final Object lock = new Object();
    private final ArrayBlockingQueue<Event> bootstrapEvents;
    private volatile TraceSink traces;
    private boolean binding;
    private boolean closed;

    public DeferredTelemetrySink(int capacity) {
        bootstrapEvents = new ArrayBlockingQueue<Event>(Math.max(1, capacity));
    }

    public void bind(TraceSink traceSink) {
        if (traceSink == null) return;
        synchronized (lock) {
            if (closed || traces != null || binding) return;
            binding = true;
        }
        while (true) {
            List<Event> pending = new ArrayList<Event>();
            synchronized (lock) {
                if (closed) {
                    binding = false;
                    bootstrapEvents.clear();
                    return;
                }
                bootstrapEvents.drainTo(pending);
                if (pending.isEmpty()) {
                    traces = traceSink;
                    binding = false;
                    return;
                }
            }
            for (Event event : pending) apply(event, traceSink);
        }
    }

    public void close() {
        synchronized (lock) {
            closed = true;
            binding = false;
            bootstrapEvents.clear();
            traces = null;
        }
    }

    public void applicationEnded(final long time) {
        submit(new Event() { @Override public void apply(TraceSink traces) { traces.applicationEnded(time); } });
    }
    public void jobStarted(final int id, final int[] stages, final long time) {
        final int[] copied = stages.clone();
        submit(new Event() { @Override public void apply(TraceSink traces) { traces.jobStarted(id, copied, time); } });
    }
    public void jobEnded(
            final int id, final long end, final String outcome, final String failure) {
        submit(new Event() { @Override public void apply(TraceSink traces) {
            traces.jobEnded(id, end, outcome, failure);
        }});
    }
    public void stageStarted(final int id, final int attempt, final long time) {
        submit(new Event() { @Override public void apply(TraceSink traces) { traces.stageStarted(id, attempt, time); } });
    }
    public void stageEnded(
            final int id, final int attempt, final long end,
            final String outcome, final String failure) {
        submit(new Event() { @Override public void apply(TraceSink traces) {
            traces.stageEnded(id, attempt, end, outcome, failure);
        }});
    }

    private void submit(Event event) {
        TraceSink current;
        synchronized (lock) {
            if (closed) return;
            current = traces;
            if (current == null) {
                bootstrapEvents.offer(event);
                return;
            }
        }
        apply(event, current);
    }

    private static void apply(Event event, TraceSink traces) {
        try {
            event.apply(traces);
        } catch (RuntimeException ignored) {
            // Spark listener delivery is fail-open.
        } catch (LinkageError ignored) {
            // Spark listener delivery is fail-open across provided-dependency ABI errors.
        }
    }

    private interface Event { void apply(TraceSink traces); }
}
