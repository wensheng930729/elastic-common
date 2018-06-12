package io.polyglotted.elastic.search;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.ListBuilder.ImmutableListBuilder;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.Verbose;
import lombok.SneakyThrows;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregations;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.common.util.ListBuilder.immutableListBuilder;
import static io.polyglotted.common.util.MapRetriever.MAP_CLASS;
import static io.polyglotted.common.util.MapRetriever.reqdStr;
import static io.polyglotted.common.util.ReflectionUtil.safeInvoke;
import static io.polyglotted.common.util.StrUtil.notNullOrEmpty;
import static io.polyglotted.elastic.search.AggsConverter.detectAgg;
import static io.polyglotted.elastic.search.AggsFlattener.flattenAggs;
import static io.polyglotted.elastic.search.QueryMaker.DEFAULT_KEEP_ALIVE;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.ElasticsearchException.getExceptionName;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

abstract class SearchUtil {

    static <T> QueryResponse.Builder responseBuilder(SearchResponse searchResponse, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        QueryResponse.Builder responseBuilder = QueryResponse.responseBuilder();
        responseBuilder.header(headerFrom(searchResponse));
        if (getReturnedHits(searchResponse) > 0) responseBuilder.results(resultBuilder.buildFrom(searchResponse, verbose));
        return responseBuilder;
    }

    static SearchResponse performScroll(ElasticClient client, AuthHeader auth, SearchResponse response) {
        return client.searchScroll(auth, new SearchScrollRequest(response.getScrollId()).scroll(DEFAULT_KEEP_ALIVE));
    }

    static void performClearScroll(ElasticClient client, AuthHeader auth, String scrollId) {
        if (notNullOrEmpty(scrollId)) {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(auth, clearScrollRequest);
        }
    }

    static ResponseHeader headerFrom(SearchResponse response) {
        return new ResponseHeader(response.getTook().millis(), getTotalHits(response), getReturnedHits(response), response.getScrollId());
    }

    static XContentBuilder headerFrom(SearchResponse response, XContentBuilder builder) throws IOException {
        builder.startObject("header").field("tookInMillis", response.getTook().millis()).field("totalHits", getTotalHits(response))
            .field("returnedHits", getReturnedHits(response));
        return response.getScrollId() != null ? builder.field("scrollId", response.getScrollId()).endObject() : builder.endObject();
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

    @SneakyThrows static SearcherException failureException(Exception ex) {
        return new SearcherException(failureContent(jsonBuilder().startObject(), ex).endObject().string());
    }

    static XContentBuilder failureContent(XContentBuilder builder, Exception ex) throws Exception {
        if (ex == null) { return builder.field("error", "unknown"); }

        ElasticsearchException[] rootCauses = localGuessRootCauses(ex);
        builder.startObject("error");
        builder.startArray("root_cause");
        for (ElasticsearchException rootCause : rootCauses) {
            builder.startObject().field("type", (String) safeInvoke(rootCause, "getExceptionName")).field("reason", rootCause.getMessage()).endObject();
        }
        return builder.endArray().field("type", getExceptionName(ex)).field("reason", ex.getMessage()).endObject();
    }

    private static ElasticsearchException[] localGuessRootCauses(Throwable t) {
        Throwable ex = ExceptionsHelper.unwrapCause(t);
        if (ex instanceof ElasticsearchException) {
            ElasticsearchException esx = (ElasticsearchException) ex;
            Throwable[] suppressed = esx.getSuppressed();
            return (suppressed.length > 0) ? buildSuppressExes(suppressed[0]) : esx.guessRootCauses();
        }
        return newElasticsearchExes(t);
    }

    private static ElasticsearchException[] buildSuppressExes(Throwable suppressed) {

        return suppressed instanceof ElasticsearchException ? new ElasticsearchException[]{(ElasticsearchException) suppressed} :
            (suppressed instanceof ResponseException ? unwrapRespEx((ResponseException) suppressed) : newElasticsearchExes(suppressed));
    }

    @SneakyThrows private static ElasticsearchException[] unwrapRespEx(ResponseException rex) {
        String message = EntityUtils.toString(rex.getResponse().getEntity());
        List<Map<String, Object>> rootCauses = deserialize(message).deepCollect("error.root_cause", MAP_CLASS);
        if (!rootCauses.isEmpty()) {
            ElasticsearchException[] result = new ElasticsearchException[rootCauses.size()]; int index = 0;
            for (Map<String, Object> rootCause : rootCauses) {
                result[index++] = newElasticsearchEx(reqdStr(rootCause, "type"), reqdStr(rootCause, "reason"), null);
            }
            return result;
        }
        else { return new ElasticsearchException[]{new ElasticsearchException(message, rex)}; }
    }

    private static ElasticsearchException[] newElasticsearchExes(Throwable t) {
        return new ElasticsearchException[]{newElasticsearchEx(getExceptionName(t), t.getMessage(), t)};
    }

    private static ElasticsearchException newElasticsearchEx(String name, String message, Throwable t) {
        return new ElasticsearchException(message, t) {
            @Override protected String getExceptionName() { return name; }
        };
    }
}