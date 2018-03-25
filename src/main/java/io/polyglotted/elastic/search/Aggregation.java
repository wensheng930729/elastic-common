package io.polyglotted.elastic.search;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.MapBuilder.ImmutableMapBuilder;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static io.polyglotted.common.model.MapResult.immutableResult;
import static io.polyglotted.common.model.SortedMapResult.treeResult;
import static io.polyglotted.common.util.Assertions.checkBool;
import static io.polyglotted.common.util.CollUtil.transform;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.common.util.ListBuilder.simpleList;
import static io.polyglotted.common.util.MapBuilder.immutableMapBuilder;
import static io.polyglotted.common.util.MapRetriever.listVal;
import static io.polyglotted.common.util.MapRetriever.mapVal;
import static io.polyglotted.common.util.MapRetriever.reqdStr;
import static io.polyglotted.common.util.MapRetriever.reqdValue;
import static io.polyglotted.elastic.search.Bucket.deserializeBucket;
import static java.util.Objects.requireNonNull;

@ToString(includeFieldNames = false, doNotUseGetters = true, of = {"label", "type", "value"})
@EqualsAndHashCode
@SuppressWarnings({"unused", "WeakerAccess"})
@RequiredArgsConstructor
public final class Aggregation {
    public final String label;
    public final String type;
    public final Object value;
    public final MapResult parameters;

    public boolean hasBuckets() { return AggregationType.valueOf(type).hasBuckets; }

    @SuppressWarnings("unchecked")
    public List<Bucket> buckets() { checkBool(hasBuckets(), type + " does not support buckets"); return (List<Bucket>) value; }

    public Map<String, Long> bucketCounts() {
        ImmutableMapBuilder<String, Long> bucketCounts = immutableMapBuilder();
        for (Bucket bucket : buckets()) bucketCounts.put(String.valueOf(bucket.value), bucket.count);
        return bucketCounts.build();
    }

    public <T> T param(String name, Class<T> tClass) { return tClass.cast(parameters.get(name)); }

    public long longValue(String name) { return value(name, Long.class); }

    public double doubleValue(String name) { return value(name, Double.class); }

    @SuppressWarnings("unchecked") public <T> T value(String name, Class<T> tClass) {
        return (value instanceof Map) ? tClass.cast(((Map) value).get(name)) : tClass.cast(value);
    }

    @SuppressWarnings("unchecked") public Iterable<Entry<String, Object>> valueIterable() {
        return (value instanceof Map) ? ((Map<String, Object>) value).entrySet() : immutableList(new SimpleEntry("value", value));
    }

    public static Aggregation.Builder deserializeAgg(MapResult result) {
        AggregationType aggType = AggregationType.valueOf(reqdStr(result, "type"));
        Aggregation.Builder builder = aggregationBuilder().label(reqdStr(result, "label")).type(aggType).params(mapVal(result, "parameters"));
        aggType.deserValue(result, builder);
        return builder;
    }

    public static Builder aggregationBuilder() { return new Builder(); }

    @Setter
    @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String label;
        private AggregationType type;
        private final MapResult valueMap = treeResult();
        private final MapResult paramsMap = treeResult();
        private final List<Bucket.Builder> builders = simpleList();

        public Builder value(String key, Object value) { valueMap.put(key, value); return this; }

        public void values(Map<String, Object> values) { valueMap.putAll(values); }

        public Builder param(String key, Object value) { paramsMap.put(key, value); return this; }

        public Builder params(Map<String, Object> params) { paramsMap.putAll(params); return this; }

        public Bucket.Builder bucketBuilder() { Bucket.Builder builder = Bucket.bucketBuilder(); this.builders.add(builder); return builder; }

        public void bucket(Bucket.Builder builder) { this.builders.add(builder); }

        public Aggregation build() {
            return new Aggregation(requireNonNull(label, "label is required"), requireNonNull(type, "type is required").name(),
                type.valueFrom(valueMap, transform(builders, Bucket.Builder::build)), immutableResult(paramsMap));
        }
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public enum AggregationType {
        Max(false, false), Min(false, false), Sum(false, false), Avg(false, false), Count(false, false), ExtStatistics(false, true),
        Statistics(false, true), Term(true, false), DateHistogram(true, false), Filter(true, false), Children(true, false),
        Nested(true, false), ReverseNested(true, false), Cardinality(false, false);

        public final boolean hasBuckets;
        public final boolean isMultiValue;

        @SuppressWarnings("unchecked") final <T> T valueFrom(MapResult valueMap, Iterable<Bucket> buckets) {
            return hasBuckets ? (T) immutableList(buckets) : (isMultiValue ? (T) immutableResult(valueMap) : (T) valueMap.get(name()));
        }

        void deserValue(MapResult result, Builder builder) {
            if (hasBuckets) {
                List<MapResult> buckets = listVal(result, "value");
                for (MapResult bucket : buckets) { builder.bucket(deserializeBucket(bucket)); }
            }
            else if (isMultiValue) { builder.values(mapVal(result, "value")); }
            else { builder.value(name(), reqdValue(result, "value")); }
        }
    }
}