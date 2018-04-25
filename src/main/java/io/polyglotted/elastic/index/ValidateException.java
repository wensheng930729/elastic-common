package io.polyglotted.elastic.index;

@SuppressWarnings({"serial", "WeakerAccess"})
public class ValidateException extends RuntimeException {
    public final int status;

    public ValidateException(int status, String message) { super(message); this.status = status; }
}