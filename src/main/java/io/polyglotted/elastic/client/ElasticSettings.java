package io.polyglotted.elastic.client;

import io.polyglotted.elastic.common.EsAuth;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import static io.polyglotted.common.util.StrUtil.notNullOrEmpty;
import static io.polyglotted.elastic.common.EsAuth.basicAuth;

@NoArgsConstructor @Accessors(chain = true) @Getter @Setter
public class ElasticSettings {
    String scheme = "https";
    String masterNodes = "localhost";
    int port = 9200;
    int retryTimeoutMillis = 300_000;
    boolean insecure = false;
    BootstrapAuth bootstrap = new BootstrapAuth();

    public ElasticSettings setBootstrap(String user, String password) { return setBootstrap(new BootstrapAuth(user, password)); }

    EsAuth bootstrapAuth() { return bootstrap.bootstrapAuth(); }

    public static ElasticSettings elasticSettings() { return new ElasticSettings(); }

    @NoArgsConstructor @AllArgsConstructor @Accessors(chain = true) @Getter @Setter
    public static class BootstrapAuth {
        String username = null;
        String password = null;

        EsAuth bootstrapAuth() { return notNullOrEmpty(username) && notNullOrEmpty(password) ? basicAuth(username, password) : null; }
    }
}