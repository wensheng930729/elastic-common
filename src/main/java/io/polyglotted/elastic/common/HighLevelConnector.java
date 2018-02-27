package io.polyglotted.elastic.common;

import com.google.common.base.Splitter;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("unused")
public class HighLevelConnector {

    @SneakyThrows
    public static ElasticClient highLevelClient(ElasticSettings settings) {
        return new EsRestClient(setCreds(settings, RestClient.builder(buildHosts(settings)).setMaxRetryTimeoutMillis(settings.retryTimeoutMillis)));
    }

    @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
    private static HttpHost[] buildHosts(ElasticSettings settings) {
        Iterable<String> masterNodes = Splitter.on(",").omitEmptyStrings().trimResults().split(settings.masterNodes);
        return toArray(transform(masterNodes, node -> new HttpHost(requireNonNull(node), settings.port, settings.scheme)), HttpHost.class);
    }

    private static RestClientBuilder setCreds(ElasticSettings settings, RestClientBuilder builder) {
        if (nonNull(settings.userName) && nonNull(settings.password)) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(settings.userName, settings.password));
            builder.setHttpClientConfigCallback(callback -> callback.setDefaultCredentialsProvider(credentialsProvider));
        }
        return builder;
    }
}