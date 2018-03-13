package io.polyglotted.elastic.client;

import com.google.common.base.Splitter;
import io.polyglotted.elastic.common.EsAuth;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
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
        Iterable<String> masterNodes = Splitter.on(",").omitEmptyStrings().trimResults().split(settings.masterNodes);
        return toArray(transform(masterNodes, node -> new HttpHost(requireNonNull(node), settings.port, settings.scheme)), HttpHost.class);
    }
}