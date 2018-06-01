package io.polyglotted.elastic.search;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.ListBuilder.ImmutableListBuilder;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.Verbose;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.util.ListBuilder.immutableListBuilder;
import static io.polyglotted.elastic.search.AggsConverter.detectAgg;
import static io.polyglotted.elastic.search.AggsFlattener.flattenAggs;
import static io.polyglotted.elastic.search.QueryMaker.DEFAULT_KEEP_ALIVE;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

abstract class SearchUtil {

    static <T> QueryResponse.Builder responseBuilder(SearchResponse searchResponse, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        QueryResponse.Builder responseBuilder = QueryResponse.responseBuilder();
        responseBuilder.header(headerFrom(searchResponse));
        if (getReturnedHits(searchResponse) > 0) responseBuilder.results(resultBuilder.buildFrom(searchResponse, verbose));
        return responseBuilder;
    }

    static SearchResponse performScroll(ElasticClient client, SearchResponse response) {
        return client.searchScroll(new SearchScrollRequest(response.getScrollId()).scroll(DEFAULT_KEEP_ALIVE));
    }

    static void clearScroll(ElasticClient client, SearchResponse response) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(response.getScrollId());
        client.clearScroll(clearScrollRequest);
    }

    static ResponseHeader headerFrom(SearchResponse response) {
        return new ResponseHeader(response.getTook().millis(), getTotalHits(response), getReturnedHits(response), response.getScrollId());
    }

    static void headerFrom(SearchResponse response, XContentBuilder builder) throws IOException {
        builder.startObject("header").field("tookInMillis", response.getTook().millis()).field("totalHits", getTotalHits(response))
            .field("returnedHits", getReturnedHits(response)).field("scrollId", response.getScrollId()).endObject();
    }

    static int getReturnedHits(SearchResponse response) { return response.getHits().getHits().length; }

    static long getTotalHits(SearchResponse response) { return response.getHits().getTotalHits(); }

    static MapResult hitSource(SearchHit hit) { return requireNonNull(hit).hasSource() ? simpleResult(hit.getSourceAsMap()) : simpleResult(); }

    static List<Aggregation> buildAggs(SearchResponse response) {
        ImmutableListBuilder<Aggregation> result = immutableListBuilder();
        Aggregations aggregations = response.getAggregations();
        if (aggregations != null) {
            for (org.elasticsearch.search.aggregations.Aggregation agg : aggregations) {
                result.add(detectAgg(agg).build());
            }
        }
        return result.build();
    }

    static XContentBuilder buildAggs(SearchResponse response, boolean flattenAgg, XContentBuilder result) throws IOException {
        Aggregations aggregations = response.getAggregations();
        if (aggregations != null) {
            return flattenAgg ? performFlatten(result, aggregations) : performInternal(result, aggregations);
        }
        return result;
    }

    private static XContentBuilder performFlatten(XContentBuilder result, Aggregations aggregations) throws IOException {
        result.startObject("flattened");
        for (org.elasticsearch.search.aggregations.Aggregation agg : aggregations) {
            Aggregation aggregation = detectAgg(agg).build();
            Iterator<List<Object>> flattened = flattenAggs(aggregation);

            result.startArray(aggregation.label);
            while (flattened.hasNext()) { result.value(flattened.next()); }
            result.endArray();
        }
        return result.endObject();
    }

    private static XContentBuilder performInternal(XContentBuilder result, Aggregations aggregations) throws IOException {
        aggregations.toXContent(result, EMPTY_PARAMS); return result;
    }
}