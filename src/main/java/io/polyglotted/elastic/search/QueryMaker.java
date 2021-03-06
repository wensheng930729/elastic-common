package io.polyglotted.elastic.search;

import io.polyglotted.elastic.common.Verbose;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortBuilder;

import java.util.List;

import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.elastic.search.ExprConverter.buildFilter;
import static org.elasticsearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE;

@Slf4j @SuppressWarnings({"unused", "WeakerAccess"})
public abstract class QueryMaker {
    public static final int DEFAULT_MAXIMUM = 10_000;
    public static final TimeValue DEFAULT_KEEP_ALIVE = timeValueMinutes(5);
    private static final WrapperModule searchModule = new WrapperModule();

    public static SearchRequest copyFrom(String repo, byte[] bytes, Verbose verbose) { return copyFrom(repo, bytes, null, verbose); }

    public static SearchRequest copyFrom(String repo, byte[] bytes, Long scroll, Verbose verbose) {
        SearchRequest request = new SearchRequest(repo == null ? Strings.EMPTY_ARRAY : new String[]{repo});
        request.indicesOptions(lenientExpandOpen());
        if (scroll != null) { request.scroll(timeValueMillis(scroll)); }

        SearchSourceBuilder src = searchModule.sourceFrom(bytes);
        if (src.fetchSource() != null) {
            String[] includes = src.fetchSource().includes(); String[] excludes = src.fetchSource().excludes();
            src.fetchSource(new FetchSourceContext(true, verbose.includeFields(includes), excludes));
        }
        request.source(src);
        return trace(request);
    }

    public static SearchRequest aggregationToRequest(String repo, AggregationBuilder builder) {
        SearchSourceBuilder source = new SearchSourceBuilder().size(0).query(matchAllQuery()).aggregation(builder);
        return trace(new SearchRequest(repo).indicesOptions(lenientExpandOpen()).source(source));
    }

    public static SearchRequest filterToScroller(String repo, Expression filter, int size) {
        return filterToRequest(repo, filter, FETCH_SOURCE, immutableList(), size).scroll(DEFAULT_KEEP_ALIVE);
    }

    public static SearchRequest filterToRequest(String repo, Expression filter, FetchSourceContext context, List<SortBuilder<?>> sorts, int size) {
        SearchSourceBuilder source = new SearchSourceBuilder().size(size).query(nonNull(buildFilter(filter), matchAllQuery())).fetchSource(context);
        for (SortBuilder<?> sort : sorts) { source.sort(sort); }
        return trace(new SearchRequest(repo).indicesOptions(lenientExpandOpen()).source(source));
    }

    public static SearchScrollRequest scrollRequest(String scrollId, TimeValue scrollTime) {
        return new SearchScrollRequest(scrollId).scroll(scrollTime);
    }

    @SneakyThrows
    private static SearchRequest trace(SearchRequest searchRequest) {
        if (log.isDebugEnabled()) { log.debug(searchRequest.source().toString()); } return searchRequest;
    }
}