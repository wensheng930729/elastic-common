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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.transform;
import static io.polyglotted.common.util.ListBuilder.immutableList;

@SuppressWarnings({"unused"})
@ToString(includeFieldNames = false, doNotUseGetters = true)
@EqualsAndHashCode @RequiredArgsConstructor
public final class SimpleResponse {
    public final ResponseHeader header;
    public final ImmutableList<Object> results;
    public final ImmutableList<Aggregation> aggregations;

    public <T> List<T> resultsAs(Class<? extends T> tClass) { return transform(results, tClass::cast); }

    public String scrollId() { return checkNotNull(header, "cannot find header in query response").scrollId; }

    public static Builder responseBuilder() { return new Builder(); }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class Builder {
        private ResponseHeader header = null;
        private final List<Object> results = new ArrayList<>();
        private List<Aggregation> aggregations = new ArrayList<>();

        Builder result(Object object) { this.results.add(object); return this; }

        Builder results(Iterable<?> objects) { Iterables.addAll(results, objects); return this; }

        void aggregation(Aggregation.Builder builder) { this.aggregations.add(builder.build()); }

        SimpleResponse build() {
            return new SimpleResponse(checkNotNull(header, "header is required"), immutableList(results), immutableList(aggregations));
        }
    }
}