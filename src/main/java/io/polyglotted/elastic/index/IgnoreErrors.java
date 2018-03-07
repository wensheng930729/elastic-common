package io.polyglotted.elastic.index;

@SuppressWarnings("unused")
public enum IgnoreErrors {
    LENIENT {
        @Override public boolean ignoreFailure(String message) { return true; }
    },
    STRICT {
        @Override public boolean ignoreFailure(String message) { return message == null; }
    },
    VERSION_CONFLICT {
        @Override public boolean ignoreFailure(String message) { return message == null || message.indexOf("VersionConflictEngineException") > 0; }
    };

    public abstract boolean ignoreFailure(String message);
}