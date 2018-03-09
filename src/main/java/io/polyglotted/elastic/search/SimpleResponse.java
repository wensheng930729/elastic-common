package io.polyglotted.elastic.search;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.transform;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static java.util.Objects.requireNonNull;

@SuppressWarnings({"unused", "WeakerAccess"})
@ToString(includeFieldNames = false, doNotUseGetters = true)
@EqualsAndHashCode @RequiredArgsConstructor
public final class SimpleResponse {
    public final ResponseHeader header;
    public final ImmutableList<Object> results;
    public final ImmutableList<Aggregation> aggregations;

    public <T> List<T> resultsAs(Class<? extends T> tClass) { return transform(results, tClass::cast); }

    public String scrollId() { return requireNonNull(header, "cannot find header in query response").scrollId; }

    public static Builder responseBuilder() { return new Builder(); }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private ResponseHeader header = null;
        private final List<Object> results = new ArrayList<>();
        private List<Aggregation> aggregations = new ArrayList<>();

        public Builder result(Object object) { this.results.add(object); return this; }

        public Builder results(Iterable<?> objects) { Iterables.addAll(results, objects); return this; }

        public void aggregation(Aggregation.Builder builder) { this.aggregations.add(builder.build()); }

        public SimpleResponse build() {
            return new SimpleResponse(requireNonNull(header, "header is required"), immutableList(results), immutableList(aggregations));
        }
    }
}