package io.polyglotted.elastic.search;

import com.google.common.collect.Iterables;
import io.polyglotted.common.model.MapResult;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.transform;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static java.util.Objects.requireNonNull;

@SuppressWarnings({"unused", "WeakerAccess"})
@RequiredArgsConstructor
public final class SimpleResponse {
    public final ResponseHeader header;
    public final List<Object> results;
    public final List<Aggregation> aggregations;

    public List<MapResult> results() { return resultsAs(MapResult.class); }

    public <T> List<T> resultsAs(Class<? extends T> tClass) { return transform(results, tClass::cast); }

    public boolean hasNextScroll() { return header.returnedHits > 0; }

    public String scrollId() { return header.scrollId; }

    public static Builder responseBuilder() { return new Builder(); }

    @Accessors(fluent = true, chain = true) @Setter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private ResponseHeader header = null;
        private final List<Object> results = new ArrayList<>();
        private List<Aggregation> aggregations = new ArrayList<>();

        public void results(Iterable<?> objects) { Iterables.addAll(results, objects); }

        public Builder result(Object object) { this.results.add(object); return this; }

        public void aggregation(Aggregation.Builder builder) { this.aggregations.add(builder.build()); }

        public SimpleResponse build() {
            return new SimpleResponse(requireNonNull(header, "header is required"), immutableList(results), immutableList(aggregations));
        }
    }
}