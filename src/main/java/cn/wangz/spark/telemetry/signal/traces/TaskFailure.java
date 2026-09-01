package cn.wangz.spark.telemetry.signal.traces;

/** Immutable, Spark-neutral failure snapshot passed synchronously to the trace pipeline. */
public final class TaskFailure {
    static final int MAX_MESSAGE_LENGTH = 8192;
    static final int MAX_STATUS_DESCRIPTION_LENGTH = 1024;
    static final int MAX_STACK_TRACE_LENGTH = 65536;

    private final String reasonType;
    private final String reasonClass;
    private final String message;
    private final boolean countsTowardsTaskFailures;
    private final Throwable exception;
    private final String exceptionType;
    private final String exceptionMessage;
    private final String exceptionStackTrace;

    public TaskFailure(
            String reasonType,
            String reasonClass,
            String message,
            boolean countsTowardsTaskFailures,
            Throwable exception,
            String exceptionType,
            String exceptionMessage,
            String exceptionStackTrace) {
        this.reasonType = safe(reasonType);
        this.reasonClass = safe(reasonClass);
        this.message = limit(message, MAX_MESSAGE_LENGTH);
        this.countsTowardsTaskFailures = countsTowardsTaskFailures;
        this.exception = exception;
        this.exceptionType = safe(exceptionType);
        this.exceptionMessage = limit(exceptionMessage, MAX_MESSAGE_LENGTH);
        this.exceptionStackTrace = limit(exceptionStackTrace, MAX_STACK_TRACE_LENGTH);
    }

    String reasonType() { return reasonType; }
    String message() { return message; }
    boolean countsTowardsTaskFailures() { return countsTowardsTaskFailures; }
    Throwable exception() { return exception; }
    String exceptionType() { return exceptionType; }
    String exceptionMessage() { return exceptionMessage; }
    String exceptionStackTrace() { return exceptionStackTrace; }

    String errorType() {
        return exceptionType.isEmpty() ? reasonClass : exceptionType;
    }

    String statusDescription() {
        String type = exceptionType.isEmpty() ? reasonType : exceptionType;
        String detail = exceptionMessage.isEmpty() ? message : exceptionMessage;
        return limit(detail.isEmpty() ? type : detail, MAX_STATUS_DESCRIPTION_LENGTH);
    }

    boolean hasExceptionDetails() {
        return exception != null
                || !exceptionType.isEmpty()
                || !exceptionMessage.isEmpty()
                || !exceptionStackTrace.isEmpty();
    }

    private static String safe(String value) { return value == null ? "" : value; }

    private static String limit(String value, int max) {
        String safe = safe(value);
        return safe.length() <= max ? safe : safe.substring(0, max);
    }
}
