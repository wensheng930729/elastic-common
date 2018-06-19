package io.polyglotted.elastic.discovery;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.client.ElasticSettings;
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
import static io.polyglotted.common.util.ListBuilder.simpleList;
import static io.polyglotted.common.util.MapRetriever.MAP_CLASS;
import static io.polyglotted.common.util.MapRetriever.deepCollect;
import static io.polyglotted.common.util.StrUtil.safePrefix;
import static java.util.Collections.emptyMap;
import static org.apache.http.HttpStatus.SC_MULTIPLE_CHOICES;
import static org.apache.http.HttpStatus.SC_OK;

@Slf4j @RequiredArgsConstructor
public final class InternalHostsSniffer implements HostsSniffer {
    private final RestClient lowLevelClient;
    private final ElasticSettings settings;
    private final AuthHeader bootstrapAuth;

    public static Sniffer buildSniffer(RestClient lowLevelClient, ElasticSettings settings, AuthHeader authHeader) {
        return Sniffer.builder(lowLevelClient).setHostsSniffer(new InternalHostsSniffer(lowLevelClient, settings, authHeader)).build();
    }

    @Override public List<HttpHost> sniffHosts() throws IOException {
        MapResult result = deserialize(performCliRequest());
        List<String> addresses = simpleList();
        for(Map.Entry<String, Object> entry : result.mapVal("nodes").entrySet()) {
            addresses.addAll(deepCollect(MAP_CLASS.cast(entry.getValue()), "http.bound_address", String.class));
        }
        return transformList(addresses, node -> new HttpHost(safePrefix(node, ":"), settings.getPort(), settings.getScheme()));
    }

    private String performCliRequest() throws IOException {
        try {
            Response response = lowLevelClient.performRequest("GET", "/_nodes/http", emptyMap(), null, bootstrapAuth.headers());
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= SC_OK && statusCode < SC_MULTIPLE_CHOICES) { log.warn("sniff failed: " + response.getStatusLine().getReasonPhrase()); }
            return EntityUtils.toString(response.getEntity());
        } catch (Exception ex) { log.error("sniff error", ex); return "{}"; }
    }
}