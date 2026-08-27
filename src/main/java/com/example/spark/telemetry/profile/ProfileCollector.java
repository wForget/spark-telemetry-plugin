package com.example.spark.telemetry.profile;

import java.time.Duration;

/** Implemented by separately packaged async-profiler/JFR collectors. */
public interface ProfileCollector extends AutoCloseable {
    interface Sink { boolean offer(ProfileBatch batch); }

    void start(Sink sink);
    void stop(Duration timeout);

    @Override
    default void close() { stop(Duration.ZERO); }
}
