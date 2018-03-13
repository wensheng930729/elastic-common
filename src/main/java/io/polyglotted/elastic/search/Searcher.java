package io.polyglotted.elastic.search;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.EsAuth;
import io.polyglotted.elastic.common.Verbose;
import io.polyglotted.elastic.index.IndexRecord;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.List;
import java.util.Map;

import static io.polyglotted.common.util.BaseSerializer.MAPPER;
import static io.polyglotted.elastic.search.Finder.findAll;
import static io.polyglotted.elastic.search.Finder.findBy;
import static io.polyglotted.elastic.search.QueryMaker.scrollRequest;
import static io.polyglotted.elastic.search.SearchUtil.buildAggs;
import static io.polyglotted.elastic.search.SearchUtil.clearScroll;
import static io.polyglotted.elastic.search.SearchUtil.getReturnedHits;
import static io.polyglotted.elastic.search.SearchUtil.getTotalHits;
import static io.polyglotted.elastic.search.SearchUtil.headerFrom;
import static io.polyglotted.elastic.search.SearchUtil.performScroll;
import static io.polyglotted.elastic.search.SearchUtil.responseBuilder;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

@SuppressWarnings({"unused", "WeakerAccess"})
@Slf4j @RequiredArgsConstructor
public final class Searcher {
    private final ElasticClient client;

    public Map<String, MapResult> getAll(EsAuth auth, List<IndexRecord.Builder> builders) { return findAll(client, auth, builders); }

    public MapResult getSourceBy(EsAuth auth, IndexRecord.Builder rec) { return findBy(client, auth, rec.index, rec.id, rec.parent); }

    public <T> T getById(EsAuth auth, String index, String id, ResultBuilder<T> resultBuilder, Verbose verbose) {
        return getById(auth, index, id, null, resultBuilder, verbose);
    }

    public <T> T getById(EsAuth auth, String index, String id, String parent, ResultBuilder<T> resultBuilder, Verbose verbose) {
        return resultBuilder.buildVerbose(findBy(client, auth, index, id, parent), verbose);
    }

    public <T> SimpleResponse searchBy(EsAuth auth, SearchRequest request, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        return responseBuilder(client.search(auth, request), resultBuilder, verbose).build();
    }

    public <T> SimpleResponse scroll(EsAuth auth, String scrollId, TimeValue scrollTime, ResponseBuilder<T> resultBuilder, Verbose verbose) {
        return responseBuilder(client.searchScroll(auth, scrollRequest(scrollId, scrollTime)), resultBuilder, verbose).build();
    }

    @SneakyThrows
    public <T> String searchNative(EsAuth auth, SearchRequest request, ResponseBuilder<T> resultBuilder, boolean flattenAgg, Verbose verbose) {
        XContentBuilder result = XContentFactory.jsonBuilder().startObject();
        SearchResponse response = client.search(auth, request);
        headerFrom(response, result);
        if (getReturnedHits(response) > 0) {
            List<T> values = resultBuilder.buildFrom(response, verbose);
            result.rawField("results", new BytesArray(MAPPER.writeValueAsBytes(values)), JSON);
        }
        return buildAggs(response, flattenAgg, result).endObject().string();
    }

    public <T> boolean simpleScroll(EsAuth auth, SearchRequest request, ResponseBuilder<T> resultBuilder, Verbose verbose, ScrollWalker<T> walker) {
        boolean errored = false;
        SearchResponse response = client.search(auth, request);
        log.info("performing scroll on " + getTotalHits(response) + " items");
        while (getReturnedHits(response) > 0) {
            errored = walker.walk(resultBuilder.buildFrom(response, verbose));
            if (errored) { clearScroll(client, auth, response); break; }
            response = performScroll(client, auth, response);
        }
        return errored;
    }
}