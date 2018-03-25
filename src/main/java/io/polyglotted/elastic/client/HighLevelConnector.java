package io.polyglotted.elastic.client;

import io.polyglotted.elastic.common.EsAuth;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import static io.polyglotted.common.util.CollUtil.transform;
import static io.polyglotted.common.util.CommaUtil.commaSplit;
import static java.util.Objects.requireNonNull;

public class HighLevelConnector {

    @SneakyThrows
    public static ElasticClient highLevelClient(ElasticSettings settings, EsAuth auth) {
        return highLevelClient(settings).waitForStatus(auth, "yellow");
    }

    @SneakyThrows
    public static ElasticClient highLevelClient(ElasticSettings settings) {
        return new ElasticRestClient(RestClient.builder(buildHosts(settings)).setMaxRetryTimeoutMillis(settings.retryTimeoutMillis));
    }

    @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
    private static HttpHost[] buildHosts(ElasticSettings settings) {
        Iterable<String> masterNodes = commaSplit(settings.masterNodes);
        return transform(masterNodes, node -> new HttpHost(requireNonNull(node), settings.port, settings.scheme)).toArray(HttpHost.class);
    }
}