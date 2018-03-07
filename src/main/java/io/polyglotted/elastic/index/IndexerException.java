package io.polyglotted.elastic.index;

import io.polyglotted.elastic.client.ElasticException;

@SuppressWarnings({"serial", "WeakerAccess"})
public class IndexerException extends ElasticException {
    public IndexerException(String message) { super(message); }

    public IndexerException(String message, Throwable cause) { super(message, cause); }
}