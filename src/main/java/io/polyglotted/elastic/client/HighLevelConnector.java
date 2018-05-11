package io.polyglotted.elastic.client;

import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import static io.polyglotted.common.util.CollUtil.transform;
import static io.polyglotted.common.util.CommaUtil.commaSplit;
import static io.polyglotted.common.util.InsecureSslFactory.insecureSslContext;
import static java.util.Objects.requireNonNull;

public class HighLevelConnector {

    @SneakyThrows public static ElasticClient highLevelClient(ElasticSettings settings) {
        RestClientBuilder restClientBuilder = RestClient.builder(buildHosts(settings)).setMaxRetryTimeoutMillis(settings.retryTimeoutMillis);
        if (settings.insecure) {
            restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                .setSSLContext(insecureSslContext(settings.masterNodes, settings.port)).setSSLHostnameVerifier(new NoopHostnameVerifier()));
        }
        return new ElasticRestClient(restClientBuilder, settings.bootstrapAuth());
    }

    @SuppressWarnings("StaticPseudoFunctionalStyleMethod") private static HttpHost[] buildHosts(ElasticSettings settings) {
        Iterable<String> masterNodes = commaSplit(settings.masterNodes);
        return transform(masterNodes, node -> new HttpHost(requireNonNull(node), settings.port, settings.scheme)).toArray(HttpHost.class);
    }
}