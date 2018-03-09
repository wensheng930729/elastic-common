package io.polyglotted.elastic.search;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.common.util.ConversionUtil.asLong;
import static io.polyglotted.common.util.MapRetriever.listVal;
import static io.polyglotted.common.util.MapRetriever.optValue;
import static io.polyglotted.common.util.MapRetriever.reqdStr;
import static io.polyglotted.common.util.MapRetriever.reqdValue;
import static io.polyglotted.elastic.search.Aggregation.deserializeAgg;

@SuppressWarnings({"WeakerAccess"})
@ToString(includeFieldNames = false, doNotUseGetters = true, of = {"key", "count", "aggregations"})
@EqualsAndHashCode @RequiredArgsConstructor
public final class Bucket {
    public final String key;
    public final Object value;
    public final long count;
    public final ImmutableList<Aggregation> aggregations;

    public boolean hasAggregations() { return aggregations.size() > 0; }

    public static Bucket.Builder deserializeBucket(Map<String, Object> map) {
        Bucket.Builder builder = bucketBuilder().key(reqdStr(map, "key")).value(reqdValue(map, "value")).count(asLong(optValue(map, "count", 0L)));
        List<Map<String, Object>> aggs = listVal(map, "aggregations");
        for (Map<String, Object> agg : aggs) { builder.aggregation(deserializeAgg(agg)); }
        return builder;
    }

    public static Builder bucketBuilder() { return new Builder(); }

    @Setter @Accessors(fluent = true)
    public static class Builder {
        private String key;
        private Object value;
        private long count;
        private final List<Aggregation.Builder> builders = new ArrayList<>();

        public void aggregation(Aggregation.Builder builder) { this.builders.add(builder); }

        public Bucket build() { return new Bucket(key, value, count, copyOf(transform(builders, Aggregation.Builder::build))); }
    }
}