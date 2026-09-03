package cn.wangz.spark.telemetry.signal.profiles;

/** Thread-bound dynamic profile context. Closing is idempotent and fail-open. */
public interface ProfileScope extends AutoCloseable {
    ProfileScope NONE = new ProfileScope() {
        @Override public void close() {}
    };

    @Override
    void close();
}
