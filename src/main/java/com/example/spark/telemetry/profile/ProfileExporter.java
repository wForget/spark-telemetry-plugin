package com.example.spark.telemetry.profile;

/** Transport SPI kept separate from the native profiler package. */
public interface ProfileExporter extends AutoCloseable {
    ExportResult export(ProfileBatch batch);

    @Override
    void close();

    final class ExportResult {
        private final boolean success;
        private final boolean retryable;
        private final int statusCode;
        private final String message;

        private ExportResult(boolean success, boolean retryable, int statusCode, String message) {
            this.success = success;
            this.retryable = retryable;
            this.statusCode = statusCode;
            this.message = message;
        }

        public static ExportResult success(int statusCode) {
            return new ExportResult(true, false, statusCode, "");
        }
        public static ExportResult failure(boolean retryable, int statusCode, String message) {
            return new ExportResult(false, retryable, statusCode, message == null ? "" : message);
        }
        public boolean success() { return success; }
        public boolean retryable() { return retryable; }
        public int statusCode() { return statusCode; }
        public String message() { return message; }
    }
}
