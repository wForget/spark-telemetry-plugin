package cn.wangz.spark.telemetry.signal.profiles;

import java.time.Duration;

/** Dependency-free lifecycle boundary around the optional Pyroscope implementation. */
public interface ProfileLifecycle {
    void close(Duration timeout);
}
