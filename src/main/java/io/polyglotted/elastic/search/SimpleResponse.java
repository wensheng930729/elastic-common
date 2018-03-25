package io.polyglotted.elastic.search;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.ListBuilder.ImmutableListBuilder;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.List;

import static io.polyglotted.common.util.CollUtil.transformList;
import static io.polyglotted.common.util.ListBuilder.immutableListBuilder;
import static java.util.Objects.requireNonNull;

@SuppressWarnings({"unused", "WeakerAccess"})
@RequiredArgsConstructor
public final class SimpleResponse {
    public final ResponseHeader header;
    public final List<Object> results;
    public final List<Aggregation> aggregations;

    public List<MapResult> results() { return resultsAs(MapResult.class); }

    public <T> List<T> resultsAs(Class<? extends T> tClass) { return transformList(results, tClass::cast); }

    public boolean hasNextScroll() { return header.returnedHits > 0; }

    public String scrollId() { return header.scrollId; }

    public static Builder responseBuilder() { return new Builder(); }

    @Accessors(fluent = true, chain = true) @Setter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private ResponseHeader header = null;
        private final ImmutableListBuilder<Object> results = immutableListBuilder();
        private final ImmutableListBuilder<Aggregation> aggregations = immutableListBuilder();

        public void results(Iterable<?> objects) { this.results.addAll(objects); }

        public Builder result(Object object) { this.results.add(object); return this; }

        public void aggregation(Aggregation.Builder builder) { this.aggregations.add(builder.build()); }

        public SimpleResponse build() {
            return new SimpleResponse(requireNonNull(header, "header is required"), results.build(), aggregations.build());
        }
    }
}