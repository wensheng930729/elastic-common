package io.polyglotted.elastic.client;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor @Getter @Setter
public class ElasticSettings {
    String scheme = "http";
    String masterNodes = "localhost";
    int port = 9200;
    int retryTimeoutMillis = 300_000;

    public static ElasticSettings elasticSettings() { return new ElasticSettings(); }
}