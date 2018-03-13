package io.polyglotted.elastic.client;

import io.polyglotted.common.util.MapBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static io.polyglotted.common.util.MapBuilder.immutableMapBuilder;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

abstract class LowLevelUtil {
    static String endpoint(SearchRequest request) { return "/" + String.join("/", request.indices()) + "/_search"; }

    //Copied from org.elasticsearch.client.Request.search
    static Map<String, String> paramsFor(SearchRequest request) {
        final MapBuilder.ImmutableMapBuilder<String, String> params = immutableMapBuilder();
        params.put("typed_keys", "true").put("routing", request.routing()).put("preference", request.preference())
            .put("expand_wildcards", "open,closed").put("search_type", request.searchType().name().toLowerCase(Locale.ROOT));
        if (request.requestCache() != null) {
            params.put("request_cache", Boolean.toString(request.requestCache()));
        }
        params.put("batched_reduce_size", Integer.toString(request.getBatchedReduceSize()));
        if (request.scroll() != null && request.scroll().keepAlive() != null) {
            params.put("scroll", request.scroll().keepAlive().getStringRep());
        }
        return params.build();
    }

    static HttpEntity entityFor(SearchRequest request) throws IOException {
        return request.source() == null ? null : new StringEntity(toXContent(request.source(), JSON, true).utf8ToString(), APPLICATION_JSON);
    }

    static SearchResponse parseResponse(String response) {
        return null;
    }
}