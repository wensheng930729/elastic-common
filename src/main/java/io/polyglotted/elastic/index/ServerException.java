package io.polyglotted.elastic.index;

import io.polyglotted.elastic.client.ElasticException;

@SuppressWarnings({"Serial"}) class ServerException extends ElasticException {
    private ServerException(String message) { super(message); }

    static ServerException serverException(String message) { return new ServerException(message); }
}