package io.polyglotted.elastic.client;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyStore;
import java.util.List;

import static io.polyglotted.common.util.CollUtil.transform;
import static io.polyglotted.common.util.CommaUtil.commaSplit;
import static io.polyglotted.common.util.InsecureSslFactory.insecureSslContext;
import static io.polyglotted.common.util.ResourceUtil.urlStream;
import static java.util.Objects.requireNonNull;

@Slf4j
public class HighLevelConnector {

    @SneakyThrows public static ElasticClient highLevelClient(ElasticSettings settings) {
        RestClientBuilder restClientBuilder = RestClient.builder(buildHosts(settings))
            .setMaxRetryTimeoutMillis(settings.retryTimeoutMillis)
            .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                .setConnectTimeout(settings.connectTimeoutMillis).setSocketTimeout(settings.socketTimeoutMillis))
            .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLHostnameVerifier(new NoopHostnameVerifier())
                .setSSLContext(settings.insecure ? insecureSslContext(settings.host, settings.port) : predeterminedContext()));

        return new ElasticRestClient(restClientBuilder, settings, settings.bootstrapAuth());
    }

    @SneakyThrows private static SSLContext predeterminedContext() {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(urlStream(HighLevelConnector.class, "elastic-stack-ca.p12"), new char[0]);
        return SSLContexts.custom().loadTrustMaterial(keyStore, new TrustSelfSignedStrategy()).build();
    }

    @SuppressWarnings("StaticPseudoFunctionalStyleMethod") private static HttpHost[] buildHosts(ElasticSettings settings) throws IOException {
        List<String> hosts = commaSplit(settings.host); log.info("received default hosts: " + hosts);
        return transform(hosts, node -> new HttpHost(requireNonNull(node), settings.port, settings.scheme)).toArray(HttpHost.class);
    }
}