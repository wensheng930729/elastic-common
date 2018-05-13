package io.polyglotted.elastic.search;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.Verbose;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.util.List;

import static io.polyglotted.common.util.BaseSerializer.MAPPER;
import static io.polyglotted.elastic.common.DocResult.docSource;
import static io.polyglotted.elastic.search.Finder.findById;
import static io.polyglotted.elastic.search.QueryMaker.scrollRequest;
import static io.polyglotted.elastic.search.SearchUtil.buildAggs;
import static io.polyglotted.elastic.search.SearchUtil.clearScroll;
import static io.polyglotted.elastic.search.SearchUtil.getReturnedHits;
import static io.polyglotted.elastic.search.SearchUtil.getTotalHits;
import static io.polyglotted.elastic.search.SearchUtil.headerFrom;
import static io.polyglotted.elastic.search.SearchUtil.performScroll;
import static io.polyglotted.elastic.search.SearchUtil.responseBuilder;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.elasticsearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE;

@SuppressWarnings({"unused", "WeakerAccess"})
@Slf4j @RequiredArgsConstructor
public final class Searcher {
    private final ElasticClient client;

    public <T> T getById(String index, String model, String id, ResultBuilder<T> resultBuilder, Verbose verbose) {
        return getById(null, index, model, id, null, FETCH_SOURCE, resultBuilder, verbose);
    }

    public <T> T getById(AuthHeader auth, String index, String model, String id, ResultBuilder<T> resultBuilder, Verbose verbose) {
        return getById(auth, index, model, id, null, FETCH_SOURCE, resultBuilder, verbose);
    }

    public <T> T getById(String index, String model, String id, String parent, ResultBuilder<T> resultBuilder, Verbose verbose) {
        return getById(null, index, model, id, parent, resultBuilder, verbose);
    }

    public <T> T getById(AuthHeader auth, String index, String model, String id, String parent, ResultBuilder<T> resultBuilder, Verbose verbose) {
        return getById(auth, index, model, id, parent, FETCH_SOURCE, resultBuilder, verbose);
    }

    public <T> T getById(String index, String model, String id, String parent, FetchSourceContext ctx, ResultBuilder<T> builder, Verbose verbose) {
        return getById(null, index, model, id, parent, ctx, builder, verbose);
    }

    public <T> T getById(AuthHeader auth, String index, String model, String id, String parent,
                         FetchSourceContext ctx, ResultBuilder<T> builder, Verbose verbose) {
        return builder.buildVerbose(docSource(findById(client, auth, index, model, id, parent, ctx)), verbose);
    }

    public <T> QueryResponse searchBy(SearchRequest request, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        return searchBy(null, request, resultBuilder, verbose);
    }

    public <T> QueryResponse searchBy(AuthHeader auth, SearchRequest request, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        return responseBuilder(auth == null ? client.search(request) : client.search(auth, request), resultBuilder, verbose).build();
    }

    public <T> QueryResponse scroll(String scrollId, TimeValue scrollTime, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        return scroll(null, scrollId, scrollTime, resultBuilder, verbose);
    }

    public <T> QueryResponse scroll(AuthHeader auth, String scrollId, TimeValue scrollTime, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        SearchScrollRequest request = scrollRequest(scrollId, scrollTime);
        return responseBuilder(auth == null ? client.searchScroll(request) : client.searchScroll(auth, request), resultBuilder, verbose).build();
    }

    public <T> String searchNative(SearchRequest request, ResponseBuilder<T> resultBuilder, boolean flattenAgg, Verbose verbose) {
        return searchNative(null, request, resultBuilder, flattenAgg, verbose);
    }

    @SneakyThrows
    public <T> String searchNative(AuthHeader auth, SearchRequest request, ResponseBuilder<T> resultBuilder, boolean flattenAgg, Verbose verbose) {
        XContentBuilder result = XContentFactory.jsonBuilder().startObject();
        SearchResponse response = auth == null ? client.search(request) : client.search(auth, request);
        headerFrom(response, result);
        if (getReturnedHits(response) > 0) {
            List<T> values = resultBuilder.buildFrom(response, verbose);
            result.rawField("results", new BytesArray(MAPPER.writeValueAsBytes(values)), JSON);
        }
        return buildAggs(response, flattenAgg, result).endObject().string();
    }

    public <T> boolean simpleScroll(SearchRequest request, ResponseBuilder<T> resultBuilder, Verbose verbose, ScrollWalker<T> walker) {
        return simpleScroll(null, request, resultBuilder, verbose, walker);
    }

    public <T> boolean simpleScroll(AuthHeader auth, SearchRequest request, ResponseBuilder<T> resultBuilder, Verbose verbose, ScrollWalker<T> walker) {
        boolean errored = false;
        SearchResponse response = auth == null ? client.search(request) : client.search(auth, request);
        log.info("performing scroll on " + getTotalHits(response) + " items");
        while (getReturnedHits(response) > 0) {
            errored = walker.walk(resultBuilder.buildFrom(response, verbose));
            if (errored) { clearScroll(client, auth, response); break; }
            response = performScroll(client, auth, response);
        }
        return errored;
    }
}