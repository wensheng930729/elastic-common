package io.polyglotted.elastic.client;

import io.polyglotted.common.model.AuthHeader;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import static io.polyglotted.common.model.AuthHeader.basicAuth;
import static io.polyglotted.common.util.StrUtil.notNullOrEmpty;

@NoArgsConstructor @Accessors(chain = true) @Getter @Setter
public class ElasticSettings {
    String scheme = "https";
    String host = "localhost";
    int port = 9200;
    int retryTimeoutMillis = 300_000;
    boolean insecure = false;
    BootstrapAuth bootstrap = new BootstrapAuth();

    public ElasticSettings setBootstrap(String user, String password) { return setBootstrap(new BootstrapAuth(user, password)); }

    AuthHeader bootstrapAuth() { return bootstrap.bootstrapAuth(); }

    public static ElasticSettings elasticSettings() { return new ElasticSettings(); }

    @NoArgsConstructor @AllArgsConstructor @Accessors(chain = true) @Getter @Setter
    public static class BootstrapAuth {
        String username = null;
        String password = null;

        AuthHeader bootstrapAuth() { return notNullOrEmpty(username) && notNullOrEmpty(password) ? basicAuth(username, password) : null; }
    }
}