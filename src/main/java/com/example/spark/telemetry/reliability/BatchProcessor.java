package com.example.spark.telemetry.reliability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/** Drains a signal queue and exports immutable batches on a daemon thread. */
public final class BatchProcessor<T> implements AutoCloseable {

    @FunctionalInterface
    public interface Exporter<T> {
        void export(List<T> batch) throws Exception;
    }

    public interface Listener {
        default void onExportSuccess(int batchSize, long durationNanos) {
        }

        default void onExportFailure(int batchSize, Throwable failure, long durationNanos) {
        }

        default void onDiscard(int itemCount) {
        }
    }

    public enum State {
        NEW,
        RUNNING,
        SHUTTING_DOWN,
        TERMINATED
    }

    private static final Listener NOOP_LISTENER = new Listener() {
    };

    private final BoundedSignalQueue<T> queue;
    private final int maxBatchSize;
    private final long flushIntervalNanos;
    private final Exporter<T> exporter;
    private final Listener listener;
    private final Object lifecycleMonitor = new Object();
    private final Object exportHandoffMonitor = new Object();
    private final Thread worker;

    private volatile State state = State.NEW;
    private volatile boolean shutdownRequested;
    private volatile boolean forceStop;
    private volatile boolean shutdownDrained = true;
    private boolean exportsClosed;

    public BatchProcessor(
            BoundedSignalQueue<T> queue,
            int maxBatchSize,
            long flushInterval,
            TimeUnit flushIntervalUnit,
            Exporter<T> exporter,
            Listener listener,
            ThreadFactory threadFactory) {
        this.queue = Objects.requireNonNull(queue, "queue");
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be greater than zero");
        }
        Objects.requireNonNull(flushIntervalUnit, "flushIntervalUnit");
        long intervalNanos = flushIntervalUnit.toNanos(flushInterval);
        if (flushInterval <= 0L || intervalNanos <= 0L) {
            throw new IllegalArgumentException("flushInterval must be greater than zero");
        }
        this.maxBatchSize = maxBatchSize;
        this.flushIntervalNanos = intervalNanos;
        this.exporter = Objects.requireNonNull(exporter, "exporter");
        this.listener = listener == null ? NOOP_LISTENER : listener;
        ThreadFactory factory = threadFactory == null
                ? daemonThreadFactory("spark-telemetry-batch")
                : threadFactory;
        Thread createdWorker = factory.newThread(this::runWorker);
        if (createdWorker == null) {
            throw new IllegalArgumentException("threadFactory returned null");
        }
        if (createdWorker.isAlive()) {
            throw new IllegalArgumentException("threadFactory returned an already-started thread");
        }
        createdWorker.setDaemon(true);
        this.worker = createdWorker;
    }

    public BatchProcessor(
            BoundedSignalQueue<T> queue,
            int maxBatchSize,
            long flushInterval,
            TimeUnit flushIntervalUnit,
            Exporter<T> exporter) {
        this(queue, maxBatchSize, flushInterval, flushIntervalUnit, exporter, null, null);
    }

    /** Starts the processor once. */
    public boolean start() {
        synchronized (lifecycleMonitor) {
            if (state != State.NEW) {
                return false;
            }
            return startWorker();
        }
    }

    public boolean offer(T element) {
        return queue.offer(element);
    }

    /** For plugin-owned collectors only; never call this from a Spark task callback. */
    public boolean offerFromBackground(T element) {
        return queue.offerFromBackground(element);
    }

    /** Stops accepting new telemetry without stopping the background worker. */
    public void stopAccepting() {
        queue.close();
    }

    /**
     * Stops accepting, drains until the deadline, and returns whether every
     * queued item was handed to the exporter before the deadline.
     *
     * <p>An exporter cannot be forcibly terminated safely. If it ignores
     * interruption this method still returns by the deadline; the worker is a
     * daemon and any items not yet handed to it are discarded.</p>
     */
    public boolean shutdown(long timeout, TimeUnit unit) {
        Objects.requireNonNull(unit, "unit");
        if (timeout < 0L) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        final long deadline = deadlineAfter(timeout, unit);
        final Thread thread = worker;

        synchronized (lifecycleMonitor) {
            queue.close();
            shutdownRequested = true;
            if (state == State.TERMINATED) {
                return shutdownDrained && queue.size() == 0;
            }
            if (state == State.NEW) {
                if (!startWorker()) {
                    return false;
                }
            }
            state = State.SHUTTING_DOWN;
            thread.interrupt();
        }

        joinUntil(thread, deadline);
        boolean terminated = state == State.TERMINATED;
        if (!terminated) {
            synchronized (exportHandoffMonitor) {
                exportsClosed = true;
                forceStop = true;
                shutdownDrained = false;
            }
            // Never wait for queue administration or invoke external listener
            // code after the deadline has elapsed. A daemon cleanup owns any
            // temporary queue-lock contention and eventually accounts drops.
            discardRemainingAsynchronously();
            thread.interrupt();
        }
        return terminated && shutdownDrained && queue.size() == 0;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit");
        long deadline = deadlineAfter(timeout, unit);
        synchronized (lifecycleMonitor) {
            while (state != State.TERMINATED) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(lifecycleMonitor, remaining);
            }
            return true;
        }
    }

    public State state() {
        return state;
    }

    public boolean isAccepting() {
        return queue.isAccepting();
    }

    @Override
    public void close() {
        shutdown(0L, TimeUnit.NANOSECONDS);
    }

    private void runWorker() {
        List<T> batch = new ArrayList<T>(maxBatchSize);
        try {
            while (!forceStop && (!shutdownRequested || queue.size() > 0)) {
                batch.clear();
                collectBatch(batch);
                if (!batch.isEmpty()) {
                    if (!tryHandoffExport()) {
                        notifyDiscard(batch.size());
                    } else {
                        exportBatch(batch);
                    }
                }
            }
        } finally {
            if (forceStop) {
                notifyDiscard(queue.discardRemaining());
            }
            synchronized (lifecycleMonitor) {
                state = State.TERMINATED;
                lifecycleMonitor.notifyAll();
            }
        }
    }

    private void collectBatch(List<T> batch) {
        try {
            T first;
            if (shutdownRequested) {
                first = queue.poll();
            } else {
                first = queue.poll(flushIntervalNanos, TimeUnit.NANOSECONDS);
            }
            if (first == null) {
                return;
            }
            batch.add(first);

            long flushDeadline = deadlineAfter(flushIntervalNanos, TimeUnit.NANOSECONDS);
            while (!forceStop && batch.size() < maxBatchSize) {
                queue.drainTo(batch, maxBatchSize - batch.size());
                if (batch.size() >= maxBatchSize || shutdownRequested) {
                    return;
                }
                long remaining = flushDeadline - System.nanoTime();
                if (remaining <= 0L) {
                    return;
                }
                T next = queue.poll(remaining, TimeUnit.NANOSECONDS);
                if (next == null) {
                    return;
                }
                batch.add(next);
            }
        } catch (InterruptedException ignored) {
            // Interruption is a wake-up mechanism for shutdown. Items already
            // placed in batch are still exported unless a hard deadline fired.
        }
    }

    private void exportBatch(List<T> batch) {
        List<T> immutableBatch = Collections.unmodifiableList(new ArrayList<T>(batch));
        long startNanos = System.nanoTime();
        try {
            exporter.export(immutableBatch);
            notifySuccess(batch.size(), System.nanoTime() - startNanos);
        } catch (Exception failure) {
            notifyFailure(batch.size(), failure, System.nanoTime() - startNanos);
        }
    }

    /** Linearization point between an export handoff and the shutdown cutoff. */
    private boolean tryHandoffExport() {
        synchronized (exportHandoffMonitor) {
            return !exportsClosed && !forceStop;
        }
    }

    /** Called only while holding lifecycleMonitor. */
    private boolean startWorker() {
        state = State.RUNNING;
        try {
            worker.start();
            return true;
        } catch (RuntimeException failure) {
            state = State.TERMINATED;
            shutdownDrained = false;
            queue.close();
            final int discarded = queue.discardRemaining();
            notifyDiscardAsynchronously(discarded);
            lifecycleMonitor.notifyAll();
            return false;
        }
    }

    private void notifySuccess(int batchSize, long durationNanos) {
        try {
            listener.onExportSuccess(batchSize, durationNanos);
        } catch (RuntimeException ignored) {
            // Listener failures must never terminate the processor.
        }
    }

    private void notifyFailure(int batchSize, Throwable failure, long durationNanos) {
        try {
            listener.onExportFailure(batchSize, failure, durationNanos);
        } catch (RuntimeException ignored) {
            // Listener failures must never terminate the processor.
        }
    }

    private void notifyDiscard(int itemCount) {
        if (itemCount <= 0) {
            return;
        }
        try {
            listener.onDiscard(itemCount);
        } catch (RuntimeException ignored) {
            // Listener failures must never escape shutdown.
        }
    }

    private void discardRemainingAsynchronously() {
        Thread cleanup = new Thread(new Runnable() {
            @Override public void run() {
                notifyDiscard(queue.discardRemaining());
            }
        }, "spark-telemetry-discard");
        cleanup.setDaemon(true);
        cleanup.start();
    }

    private void notifyDiscardAsynchronously(final int itemCount) {
        if (itemCount <= 0) return;
        Thread notification = new Thread(new Runnable() {
            @Override public void run() { notifyDiscard(itemCount); }
        }, "spark-telemetry-discard-notification");
        notification.setDaemon(true);
        notification.start();
    }

    private static ThreadFactory daemonThreadFactory(final String threadName) {
        return runnable -> {
            Thread thread = new Thread(runnable, threadName);
            thread.setDaemon(true);
            return thread;
        };
    }

    private static long deadlineAfter(long timeout, TimeUnit unit) {
        long now = System.nanoTime();
        long nanos = unit.toNanos(timeout);
        if (nanos > 0L && now > Long.MAX_VALUE - nanos) {
            return Long.MAX_VALUE;
        }
        return now + Math.max(0L, nanos);
    }

    private static void joinUntil(Thread thread, long deadline) {
        if (thread == null) {
            return;
        }
        boolean interrupted = false;
        try {
            while (thread.isAlive()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    return;
                }
                try {
                    TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
                } catch (InterruptedException e) {
                    interrupted = true;
                    return;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
