package io.polyglotted.elastic.search;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.polyglotted.common.util.CollUtil.transform;
import static io.polyglotted.common.util.ListBuilder.immutableListBuilder;
import static java.util.Arrays.asList;

abstract class AggsFlattener {

    static Iterator<List<Object>> flattenAggs(Aggregation aggs) { return flattenAggs(new String[0], aggs).iterator(); }

    private static Stream<List<Object>> flattenAggs(final String[] strings, Aggregation aggs) {
        if (!aggs.hasBuckets()) {
            return Stream.of(build(strings, aggs.valueIterable()));
        }
        if (aggs.buckets().isEmpty()) {
            final String[] inner = makeArray(strings, aggs.label);
            return Stream.of(build(inner, 0));
        }
        return aggs.buckets().stream().flatMap(bucket -> {
            final String[] inner = makeArray(strings, bucket.key);
            return !bucket.hasAggregations() ? Stream.of(build(inner, bucket.count)) :
                bucket.aggregations.stream().flatMap(child -> flattenAggs(inner, child));
        });
    }

    private static String[] makeArray(String[] strings, String key) {
        String[] result = new String[strings.length + 1];
        System.arraycopy(strings, 0, result, 0, strings.length);
        result[strings.length] = key;
        return result;
    }

    private static List<Object> build(String[] strings, long docCount) {
        return immutableListBuilder().addAll(asList((Object[]) strings)).add(docCount).build();
    }

    private static List<Object> build(String[] strings, Iterable<Map.Entry<String, Object>> aggs) {
        return immutableListBuilder().addAll(asList((Object[]) strings))
            .addAll(transform(aggs, Map.Entry::getValue)).build();
    }
}