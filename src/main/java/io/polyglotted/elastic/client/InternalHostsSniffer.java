package io.polyglotted.elastic.client;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.ListBuilder.SimpleListBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.sniff.HostsSniffer;
import org.elasticsearch.client.sniff.Sniffer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.common.util.CollUtil.transformList;
import static io.polyglotted.common.util.ListBuilder.simpleListBuilder;
import static io.polyglotted.common.util.MapRetriever.MAP_CLASS;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.http.HttpStatus.SC_MULTIPLE_CHOICES;
import static org.apache.http.HttpStatus.SC_OK;

@Slf4j @RequiredArgsConstructor
public final class InternalHostsSniffer implements HostsSniffer {
    private final RestClient lowLevelClient;
    private final ElasticSettings settings;
    private final AuthHeader bootstrapAuth;

    static Sniffer buildSniffer(RestClient lowLevelClient, ElasticSettings settings, AuthHeader authHeader) {
        return Sniffer.builder(lowLevelClient).setHostsSniffer(new InternalHostsSniffer(lowLevelClient, settings, authHeader)).build();
    }

    @Override public List<HttpHost> sniffHosts() throws IOException {
        MapResult result = deserialize(performCliRequest());
        SimpleListBuilder<String> addresses = simpleListBuilder();
        for (Map.Entry<String, Object> entry : result.mapVal("nodes").entrySet()) {
            addresses.add((String) MAP_CLASS.cast(entry.getValue()).get("ip"));
        }
        return transformList(addresses.build(), node -> new HttpHost(requireNonNull(node), settings.getPort(), settings.getScheme()));
    }

    private String performCliRequest() throws IOException {
        try {
            Response response = lowLevelClient.performRequest("GET", "/_nodes/http", emptyMap(), null, bootstrapAuth.headers());
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= SC_OK && statusCode < SC_MULTIPLE_CHOICES) { return EntityUtils.toString(response.getEntity()); }
            log.warn("sniff failed: " + response.getStatusLine().getReasonPhrase());

        } catch (Exception ex) { log.error("sniff error", ex); }
        return "{}";
    }
}