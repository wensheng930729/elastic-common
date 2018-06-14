package io.polyglotted.elastic.client;

import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import java.security.KeyStore;

import static io.polyglotted.common.util.CollUtil.transform;
import static io.polyglotted.common.util.CommaUtil.commaSplit;
import static io.polyglotted.common.util.InsecureSslFactory.insecureSslContext;
import static io.polyglotted.common.util.ResourceUtil.urlStream;
import static java.util.Objects.requireNonNull;

public class HighLevelConnector {

    @SneakyThrows public static ElasticClient highLevelClient(ElasticSettings settings) {
        RestClientBuilder restClientBuilder = RestClient.builder(buildHosts(settings)).setMaxRetryTimeoutMillis(settings.retryTimeoutMillis);
        restClientBuilder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
            .setConnectTimeout(settings.connectTimeoutMillis).setSocketTimeout(settings.socketTimeoutMillis));
        if (settings.insecure) {
            restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                .setSSLContext(insecureSslContext(settings.host, settings.port)).setSSLHostnameVerifier(new NoopHostnameVerifier()));
        } else {
            restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                .setSSLContext(predeterminedContext()).setSSLHostnameVerifier(new NoopHostnameVerifier()));
        }
        return new ElasticRestClient(restClientBuilder, settings.bootstrapAuth());
    }

    @SneakyThrows private static SSLContext predeterminedContext() {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(urlStream(HighLevelConnector.class, "elastic-stack-ca.p12"), new char[0]);
        return SSLContexts.custom().loadTrustMaterial(keyStore, new TrustSelfSignedStrategy()).build();
    }

    @SuppressWarnings("StaticPseudoFunctionalStyleMethod") private static HttpHost[] buildHosts(ElasticSettings settings) {
        Iterable<String> hosts = commaSplit(settings.host);
        return transform(hosts, node -> new HttpHost(requireNonNull(node), settings.port, settings.scheme)).toArray(HttpHost.class);
    }
}