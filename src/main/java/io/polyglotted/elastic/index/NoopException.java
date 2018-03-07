package io.polyglotted.elastic.index;

@SuppressWarnings({"serial", "WeakerAccess"})
public class NoopException extends RuntimeException {
    NoopException(String message) { super(message); }
}