package io.polyglotted.elastic.search;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.transform;

abstract class AggsFlattener {

    static Iterator<ImmutableList<Object>> flattenAggs(Aggregation aggs) { return flattenAggs(new String[0], aggs).iterator(); }

    private static Stream<ImmutableList<Object>> flattenAggs(final String[] strings, Aggregation aggs) {
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

    private static ImmutableList<Object> build(String[] strings, long docCount) {
        final List<Object> values = Lists.newArrayList((Object[]) strings);
        values.add(docCount);
        return ImmutableList.copyOf(values);
    }

    private static ImmutableList<Object> build(String[] strings, Iterable<Map.Entry<String, Object>> aggs) {
        final List<Object> values = Lists.newArrayList((Object[]) strings);
        Iterables.addAll(values, transform(aggs, Map.Entry::getValue));
        return ImmutableList.copyOf(values);
    }
}