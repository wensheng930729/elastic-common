package io.polyglotted.elastic.client;

@SuppressWarnings("Serial")
public class ElasticException extends RuntimeException {

    protected ElasticException(String message) { super(message); }

    protected ElasticException(String message, Throwable cause) { super(message, cause); }

    static void checkState(boolean status, String message) { if (!status) throw new ElasticException(message); }

    static ElasticException throwEx(String message, Throwable cause) {
        return (cause instanceof ElasticException) ? (ElasticException) cause : new ElasticException(message + ": " + cause.getMessage(), cause);
    }
}