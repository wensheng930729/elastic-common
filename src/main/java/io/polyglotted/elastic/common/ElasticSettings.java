package io.polyglotted.elastic.common;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@RequiredArgsConstructor
public final class ElasticSettings {
    final String scheme;
    final String masterNodes;
    final int port;
    final String userName;
    final String password;
    final int retryTimeoutMillis;


    public static Builder esSettingsBuilder() {
        return new Builder();
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        @NonNull
        private String scheme = "http";
        @NonNull
        private String masterNodes = "localhost";
        private int port = 9200;
        private String userName = null;
        private String password = null;
        private int retryTimeout = 300_000;

        public ElasticSettings build() {
            return new ElasticSettings(scheme, masterNodes, port, userName, password, retryTimeout);
        }
    }
}