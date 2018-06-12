package io.polyglotted.elastic.search;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.Verbose;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.List;

import static io.polyglotted.common.util.BaseSerializer.MAPPER;
import static io.polyglotted.common.util.CollUtil.transformList;
import static io.polyglotted.elastic.common.DocResult.docSource;
import static io.polyglotted.elastic.search.Finder.findAllBy;
import static io.polyglotted.elastic.search.Finder.findBy;
import static io.polyglotted.elastic.search.Finder.findById;
import static io.polyglotted.elastic.search.QueryMaker.aggregationToRequest;
import static io.polyglotted.elastic.search.QueryMaker.scrollRequest;
import static io.polyglotted.elastic.search.SearchUtil.buildAggs;
import static io.polyglotted.elastic.search.SearchUtil.failureContent;
import static io.polyglotted.elastic.search.SearchUtil.failureException;
import static io.polyglotted.elastic.search.SearchUtil.getReturnedHits;
import static io.polyglotted.elastic.search.SearchUtil.getTotalHits;
import static io.polyglotted.elastic.search.SearchUtil.headerFrom;
import static io.polyglotted.elastic.search.SearchUtil.performClearScroll;
import static io.polyglotted.elastic.search.SearchUtil.performScroll;
import static io.polyglotted.elastic.search.SearchUtil.responseBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.elasticsearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE;

@SuppressWarnings({"unused", "WeakerAccess"})
@Slf4j @RequiredArgsConstructor
public final class Searcher {
    private final ElasticClient client;

    public <T> List<T> getAllBy(String repo, String model, Expression expr, int size, FetchSourceContext context,
                                ResultBuilder<T> resultBuilder, Verbose verbose) {
        return getAllBy(null, repo, model, expr, size, context, resultBuilder, verbose);
    }

    public <T> List<T> getAllBy(AuthHeader auth, String repo, String model, Expression expr, int size,
                                FetchSourceContext context, ResultBuilder<T> builder, Verbose verbose) {
        return transformList(findAllBy(client, auth, repo, expr, size, context), docResult -> builder.buildVerbose(docSource(docResult), verbose));
    }

    public <T> T getById(String repo, String model, String id, ResultBuilder<T> resultBuilder, Verbose verbose) {
        return getById(null, repo, model, id, null, FETCH_SOURCE, resultBuilder, verbose);
    }

    public <T> T getById(AuthHeader auth, String repo, String model, String id, ResultBuilder<T> resultBuilder, Verbose verbose) {
        return getById(auth, repo, model, id, null, FETCH_SOURCE, resultBuilder, verbose);
    }

    public <T> T getById(String repo, String model, String id, String parent, ResultBuilder<T> resultBuilder, Verbose verbose) {
        return getById(null, repo, model, id, parent, resultBuilder, verbose);
    }

    public <T> T getById(AuthHeader auth, String repo, String model, String id, String parent, ResultBuilder<T> resultBuilder, Verbose verbose) {
        return getById(auth, repo, model, id, parent, FETCH_SOURCE, resultBuilder, verbose);
    }

    public <T> T getById(String repo, String model, String id, String parent, FetchSourceContext ctx, ResultBuilder<T> builder, Verbose verbose) {
        return getById(null, repo, model, id, parent, ctx, builder, verbose);
    }

    public <T> T getById(AuthHeader auth, String repo, String model, String id, String parent,
                         FetchSourceContext ctx, ResultBuilder<T> builder, Verbose verbose) {
        return builder.buildVerbose(docSource(findById(client, auth, repo, model, id, parent, ctx)), verbose);
    }

    public <T> T getByExpr(String repo, Expression expr, FetchSourceContext ctx, ResultBuilder<T> builder, Verbose verbose) {
        return getByExpr(null, repo, expr, ctx, builder, verbose);
    }

    public <T> T getByExpr(AuthHeader auth, String repo, Expression expr, FetchSourceContext ctx, ResultBuilder<T> builder, Verbose verbose) {
        return builder.buildVerbose(docSource(findBy(client, auth, repo, expr, ctx)), verbose);
    }

    public Aggregation aggregate(String repo, AggregationBuilder builder) {
        try {
            return buildAggs(client.search(aggregationToRequest(repo, builder))).get(0);
        } catch (Exception ex) { throw failureException(ex); }
    }

    public <T> QueryResponse searchBy(SearchRequest request, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        return searchBy(null, request, resultBuilder, verbose);
    }

    public <T> QueryResponse searchBy(AuthHeader auth, SearchRequest request, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        try {
            return responseBuilder(client.search(auth, request), resultBuilder, verbose).build();
        } catch (Exception ex) { throw failureException(ex); }
    }

    public <T> QueryResponse scroll(String scrollId, TimeValue scrollTime, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        return scroll(null, scrollId, scrollTime, resultBuilder, verbose);
    }

    public <T> QueryResponse scroll(AuthHeader auth, String scrollId, TimeValue scrollTime, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        try {
            SearchScrollRequest request = scrollRequest(scrollId, scrollTime);
            return responseBuilder(client.searchScroll(auth, request), resultBuilder, verbose).build();
        } catch (Exception ex) { throw failureException(ex); }
    }

    public <T> String searchNative(SearchRequest request, ResponseBuilder<T> resultBuilder, boolean flattenAgg, Verbose verbose) {
        return searchNative(null, request, resultBuilder, flattenAgg, verbose);
    }

    @SneakyThrows
    public <T> String searchNative(AuthHeader auth, SearchRequest request, ResponseBuilder<T> resultBuilder, boolean flattenAgg, Verbose verbose) {
        try {
            XContentBuilder result = jsonBuilder().startObject();
            return handleResponse(client.search(request), result, resultBuilder, true, flattenAgg, verbose).endObject().string();
        } catch (Exception ex) { throw failureException(ex); }
    }

    @SneakyThrows public <T> String multiSearch(MultiSearchRequest request, ResponseBuilder<T> resultBuilder,
                                                boolean addHeader, boolean flattenAgg, Verbose verbose) {
        XContentBuilder result = jsonBuilder().startObject().startArray("responses");
        MultiSearchResponse multiResponse = client.multiSearch(request);
        for (MultiSearchResponse.Item item : multiResponse) {
            result.startObject();
            if (item.isFailure()) {
                failureContent(result, item.getFailure()).field("status", ExceptionsHelper.status(item.getFailure()).name().toLowerCase());
            }
            else {
                handleResponse(item.getResponse(), result, resultBuilder, addHeader, flattenAgg, verbose);
                result.field("status", item.getResponse().status().name().toLowerCase());
            }
            result.endObject();
        }
        return result.endArray().endObject().string();
    }

    private <T> XContentBuilder handleResponse(SearchResponse response, XContentBuilder result, ResponseBuilder<T> resultBuilder,
                                               boolean addHeader, boolean flattenAgg, Verbose verbose) throws IOException {
        if (addHeader) { headerFrom(response, result); }
        if (getReturnedHits(response) > 0) {
            List<T> values = resultBuilder.buildFrom(response, verbose);
            result.rawField("results", new BytesArray(MAPPER.writeValueAsBytes(values)), JSON);
        }
        return buildAggs(response, flattenAgg, result);
    }

    public <T> boolean simpleScroll(SearchRequest request, ResponseBuilder<T> resultBuilder, Verbose verbose, ScrollWalker<T> walker) {
        return simpleScroll(null, request, resultBuilder, verbose, walker);
    }

    public <T> boolean simpleScroll(AuthHeader auth, SearchRequest request, ResponseBuilder<T> resultBuilder, Verbose verbose, ScrollWalker<T> walker) {
        try {
            boolean errored = false;
            SearchResponse response = client.search(auth, request);
            log.info("performing scroll on " + getTotalHits(response) + " items");
            while (getReturnedHits(response) > 0) {

                errored = walker.walk(resultBuilder.buildFrom(response, verbose));
                if (errored) { performClearScroll(client, auth, response.getScrollId()); break; }
                response = performScroll(client, auth, response);
            }
            return errored;
        } catch (Exception ex) { throw failureException(ex); }
    }

    public void clearScroll(AuthHeader auth, String scrollId) { performClearScroll(client, auth, scrollId);}
}