package io.polyglotted.elastic.search;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.Iterator;

import static io.polyglotted.elastic.search.AggsConverter.detectAgg;
import static io.polyglotted.elastic.search.AggsFlattener.flattenAggs;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;

abstract class AggsBuilder {
    static void buildAggs(SearchResponse response, boolean flattenAgg, XContentBuilder result) throws IOException {
        Aggregations aggregations = response.getAggregations();
        if (aggregations != null) {
            if (flattenAgg) { performFlatten(result, aggregations); }
            else { performInternal(result, aggregations); }
        }
    }

    private static void performFlatten(XContentBuilder result, Aggregations aggregations) throws IOException {
        result.startObject("flattened");
        for (org.elasticsearch.search.aggregations.Aggregation agg : aggregations) {
            Aggregation aggregation = detectAgg(agg).build();
            Iterator<ImmutableList<Object>> flattened = flattenAggs(aggregation);

            result.startArray(aggregation.label);
            while (flattened.hasNext()) { result.value(flattened.next()); }
            result.endArray();
        }
        result.endObject();
    }

    private static void performInternal(XContentBuilder result, Aggregations aggregations) throws IOException {
        result.startObject("aggregations");
        for (org.elasticsearch.search.aggregations.Aggregation agg : aggregations) {
            result.startObject(agg.getName());
            ((InternalAggregation) agg).doXContentBody(result, EMPTY_PARAMS);
            result.endObject();
        }
        result.endObject();
    }
}