package com.example.spark.telemetry.reliability;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** A bounded queue whose producer operation never waits for capacity or a lock. */
public final class BoundedSignalQueue<T> {

    /**
     * Selectable O(1) full-queue behavior. More policies can be added without
     * exposing queue internals or running arbitrary code on Spark task threads.
     */
    public static final class OverflowPolicy<T> {
        private enum Action { DROP_NEWEST, DROP_OLDEST }

        private final Action action;

        private OverflowPolicy(Action action) {
            this.action = action;
        }
    }

    private final int capacity;
    private final OverflowPolicy<T> overflowPolicy;
    private final Deque<T> elements;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final LongAdder accepted = new LongAdder();
    private final LongAdder dropped = new LongAdder();

    public BoundedSignalQueue(int capacity, OverflowPolicy<T> overflowPolicy) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be greater than zero");
        }
        this.capacity = capacity;
        this.overflowPolicy = Objects.requireNonNull(overflowPolicy, "overflowPolicy");
        this.elements = new ArrayDeque<T>(capacity);
    }

    public BoundedSignalQueue(int capacity) {
        this(capacity, BoundedSignalQueue.<T>dropNewest());
    }

    /**
     * Offers without waiting. Lock contention is treated as overload, even if
     * capacity remains, so a Spark callback can never queue behind a consumer.
     */
    public boolean offer(T element) {
        if (element == null || !accepting.get()) {
            return false;
        }
        if (!lock.tryLock()) {
            dropped.increment();
            return false;
        }
        try {
            return offerWhileLocked(element);
        } finally {
            if (!accepting.get()) {
                notEmpty.signalAll();
            }
            lock.unlock();
        }
    }

    /**
     * Offer for plugin-owned collector/background threads. Unlike {@link #offer(Object)}, this may
     * wait briefly for the queue lock, but it never waits for capacity and must not be used from a
     * Spark task callback.
     */
    public boolean offerFromBackground(T element) {
        if (element == null || !accepting.get()) return false;
        lock.lock();
        try {
            return offerWhileLocked(element);
        } finally {
            if (!accepting.get()) notEmpty.signalAll();
            lock.unlock();
        }
    }

    public T poll() {
        lock.lock();
        try {
            return elements.pollFirst();
        } finally {
            lock.unlock();
        }
    }

    /** Used by background consumers; producers remain non-blocking. */
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit");
        long remainingNanos = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (elements.isEmpty()) {
                if (remainingNanos <= 0L) {
                    return null;
                }
                remainingNanos = notEmpty.awaitNanos(remainingNanos);
            }
            return elements.pollFirst();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes under the queue lock, then invokes the target collection outside
     * it so external collection code cannot stall producers or shutdown.
     */
    public int drainTo(Collection<? super T> target, int maxElements) {
        Objects.requireNonNull(target, "target");
        if (maxElements <= 0) {
            return 0;
        }
        List<T> drained = new ArrayList<T>(Math.min(maxElements, capacity));
        lock.lock();
        try {
            while (drained.size() < maxElements) {
                T element = elements.pollFirst();
                if (element == null) {
                    break;
                }
                drained.add(element);
            }
        } finally {
            lock.unlock();
        }
        target.addAll(drained);
        return drained.size();
    }

    /** Stops all future offers. Elements already in the queue remain drainable. */
    public void close() {
        accepting.set(false);
        if (lock.tryLock()) {
            try {
                notEmpty.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    /** Discards queued elements, waiting only on background/administrative paths. */
    public int discardRemaining() {
        lock.lock();
        try {
            return discardWhileLocked();
        } finally {
            lock.unlock();
        }
    }

    /** Returns {@code -1} rather than waiting when another queue operation is active. */
    public int tryDiscardRemaining() {
        if (!lock.tryLock()) {
            return -1;
        }
        try {
            return discardWhileLocked();
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return elements.size();
        } finally {
            lock.unlock();
        }
    }

    public int capacity() {
        return capacity;
    }

    public boolean isAccepting() {
        return accepting.get();
    }

    public long acceptedCount() {
        return accepted.sum();
    }

    public long droppedCount() {
        return dropped.sum();
    }

    private int discardWhileLocked() {
        int count = elements.size();
        elements.clear();
        dropped.add(count);
        return count;
    }

    private boolean offerWhileLocked(T element) {
        if (!accepting.get()) return false;
        if (elements.size() >= capacity) {
            if (overflowPolicy.action == OverflowPolicy.Action.DROP_NEWEST) {
                dropped.increment();
                return false;
            }
            elements.removeFirst();
            dropped.increment();
        }
        elements.addLast(element);
        accepted.increment();
        notEmpty.signal();
        return true;
    }

    public static <T> OverflowPolicy<T> dropNewest() {
        return new OverflowPolicy<T>(OverflowPolicy.Action.DROP_NEWEST);
    }

    public static <T> OverflowPolicy<T> dropOldest() {
        return new OverflowPolicy<T>(OverflowPolicy.Action.DROP_OLDEST);
    }
}
