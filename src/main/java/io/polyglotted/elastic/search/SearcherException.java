package io.polyglotted.elastic.search;

import io.polyglotted.elastic.client.ElasticException;

@SuppressWarnings({"serial", "WeakerAccess"})
public class SearcherException extends ElasticException {
    public SearcherException(String message) { super(message); }
}
