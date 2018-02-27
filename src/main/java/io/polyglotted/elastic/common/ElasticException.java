package io.polyglotted.elastic.common;

@SuppressWarnings({"WeakerAccess", "Serial"})
public class ElasticException extends RuntimeException {

    public ElasticException(String message) { super(message); }

    public ElasticException(String message, Throwable cause) { super(message, cause); }

    public static void checkState(boolean status, String message) { if (!status) throw new ElasticException(message); }

    public static ElasticException handleEx(String message, Throwable cause) {
        return (cause instanceof ElasticException) ? (ElasticException) cause : new ElasticException(message + ": " + cause.getMessage(), cause);
    }
}