package io.polyglotted.elastic.search;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.MapRetriever;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

import static io.polyglotted.common.util.CollUtil.transformList;
import static io.polyglotted.common.util.ConversionUtil.asLong;
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
    public final List<Aggregation> aggregations;

    public boolean hasAggregations() { return aggregations.size() > 0; }

    public static Bucket.Builder deserializeBucket(MapResult map) {
        Bucket.Builder builder = bucketBuilder().key(reqdStr(map, "key")).value(reqdValue(map, "value")).count(asLong(optValue(map, "count", 0L)));
        for (MapResult agg : MapRetriever.<MapResult>listVal(map, "aggregations")) { builder.aggregation(deserializeAgg(agg)); }
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

        public Bucket build() { return new Bucket(key, value, count, transformList(builders, Aggregation.Builder::build)); }
    }
}