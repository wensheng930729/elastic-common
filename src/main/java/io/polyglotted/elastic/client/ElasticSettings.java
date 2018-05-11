package io.polyglotted.elastic.client;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@NoArgsConstructor @Accessors(chain = true) @Getter @Setter
public class ElasticSettings {
    String scheme = "http";
    String masterNodes = "localhost";
    int port = 9200;
    int retryTimeoutMillis = 300_000;
    boolean insecure = false;

    public static ElasticSettings elasticSettings() { return new ElasticSettings(); }
}