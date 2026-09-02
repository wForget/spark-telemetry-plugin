package cn.wangz.spark.telemetry.signal.profiles;

import java.time.Duration;

/** Lifecycle boundary that keeps Pyroscope startup and shutdown out of the core runtime. */
public interface ProfileLifecycle {
    void close(Duration timeout);
}
