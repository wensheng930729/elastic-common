package io.polyglotted.elastic.search;

import io.polyglotted.elastic.common.Verbose;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
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

@Slf4j @SuppressWarnings({"unused", "WeakerAccess"})
public abstract class QueryMaker {
    public static final int DEFAULT_MAXIMUM = 10_000;
    public static final TimeValue DEFAULT_KEEP_ALIVE = timeValueMinutes(5);
    private static final WrapperModule searchModule = new WrapperModule();

    public static SearchRequest copyFrom(String index, byte[] bytes, Expression additional, Long scroll, Verbose verbose) {
        SearchRequest request = new SearchRequest(index == null ? Strings.EMPTY_ARRAY : new String[]{index});
        SearchSourceBuilder src = searchModule.sourceFrom(bytes);
        SearchSourceBuilder builder = new SearchSourceBuilder().fetchSource(true);
        request.indicesOptions(lenientExpandOpen());
        builder.timeout(src.timeout());

        BoolQueryBuilder finalQuery = new BoolQueryBuilder();
        if (src.query() != null) { finalQuery.must(src.query()); }
        QueryBuilder filterExpr = buildFilter(additional);
        if (filterExpr != null) { finalQuery.filter(filterExpr); }
        builder.query(finalQuery);
        builder.highlighter(src.highlighter());

        if (src.aggregations() != null) {
            for (AggregationBuilder aggs : src.aggregations().getAggregatorFactories()) { builder.aggregation(aggs); }
        }
        if (src.sorts() != null) {
            for (SortBuilder<?> sort : src.sorts()) { builder.sort(sort); }
        }
        if (src.fetchSource() != null) {
            builder.fetchSource(new FetchSourceContext(true, verbose.includeFields(src.fetchSource().includes()), src.fetchSource().excludes()));
        }
        builder.size(src.size());
        if (scroll != null) { request.scroll(timeValueMillis(scroll)); }
        else { builder.from(src.from() < 0 ? 0 : src.from()); }
        request.source(builder);
        return trace(request);
    }

    public static SearchRequest filterToScroller(Expression filter, int size, String repo, String... models) {
        return filterToRequest(filter, immutableList(), size, repo).types(models).scroll(DEFAULT_KEEP_ALIVE);
    }

    public static SearchRequest filterToRequest(Expression filter, List<SortBuilder<?>> sorts, int size, String... repos) {
        SearchSourceBuilder source = new SearchSourceBuilder().size(size).query(nonNull(buildFilter(filter), matchAllQuery()));
        for (SortBuilder<?> sort : sorts) { source.sort(sort); }
        return trace(new SearchRequest(repos).indicesOptions(lenientExpandOpen()).source(source));
    }

    static SearchScrollRequest scrollRequest(String scrollId, TimeValue scrollTime) { return new SearchScrollRequest(scrollId).scroll(scrollTime); }

    @SneakyThrows
    private static SearchRequest trace(SearchRequest searchRequest) {
        if (log.isDebugEnabled()) { log.debug(searchRequest.source().toString()); } return searchRequest;
    }
}