package io.polyglotted.elastic.search;

import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.EsAuth;
import io.polyglotted.elastic.common.Verbose;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static io.polyglotted.elastic.search.QueryMaker.DEFAULT_KEEP_ALIVE;

@SuppressWarnings("WeakerAccess") public abstract class SearchUtil {

    public static <T> SimpleResponse.Builder responseBuilder(SearchResponse searchResponse, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        SimpleResponse.Builder responseBuilder = SimpleResponse.responseBuilder();
        responseBuilder.header(headerFrom(searchResponse));
        if (getReturnedHits(searchResponse) > 0) responseBuilder.results(resultBuilder.buildFrom(searchResponse, verbose));
        return responseBuilder;
    }

    public static SearchResponse performScroll(ElasticClient client, EsAuth auth, SearchResponse response) {
        SearchScrollRequest scrollRequest = new SearchScrollRequest(response.getScrollId()).scroll(DEFAULT_KEEP_ALIVE);
        return client.searchScroll(auth, scrollRequest);
    }

    public static void clearScroll(ElasticClient client, EsAuth auth, SearchResponse response) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(response.getScrollId());
        client.clearScroll(auth, clearScrollRequest);
    }

    public static ResponseHeader headerFrom(SearchResponse response) {
        return new ResponseHeader(response.getTook().millis(), getTotalHits(response), getReturnedHits(response), response.getScrollId());
    }

    public static void headerFrom(SearchResponse response, XContentBuilder builder) throws IOException {
        builder.startObject("header").field("tookInMillis", response.getTook().millis()).field("totalHits", getTotalHits(response))
            .field("returnedHits", getReturnedHits(response)).field("scrollId", response.getScrollId()).endObject();
    }

    public static int getReturnedHits(SearchResponse response) { return response.getHits().getHits().length; }

    public static long getTotalHits(SearchResponse response) { return response.getHits().getTotalHits(); }
}
