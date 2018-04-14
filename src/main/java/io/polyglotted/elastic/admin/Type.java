package io.polyglotted.elastic.admin;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import io.polyglotted.common.model.MapResult.ImmutableResult;
import io.polyglotted.common.util.ListBuilder;
import io.polyglotted.common.util.MapBuilder.ImmutableMapBuilder;
import io.polyglotted.elastic.admin.Field.FieldBuilder;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import static com.google.common.collect.Multimaps.asMap;
import static io.polyglotted.common.model.MapResult.immutableResultBuilder;
import static io.polyglotted.common.util.CollUtil.filterColl;
import static io.polyglotted.common.util.CollUtil.transformColl;
import static io.polyglotted.common.util.ListBuilder.immutableSet;
import static io.polyglotted.common.util.MapBuilder.immutableMap;
import static io.polyglotted.elastic.admin.TypeSerializer.serializeType;
import static io.polyglotted.elastic.common.MetaFields.AUTO_COMPLETE_FIELD;
import static java.util.Collections.singleton;

@SuppressWarnings({"unused", "WeakerAccess"})
@ToString(includeFieldNames = false, doNotUseGetters = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Type {
    public final String type = "_doc";
    public final boolean strict;
    public final boolean enabled;
    public final boolean enableSource;
    public final boolean enableSize;
    public final boolean enableAll;
    public final boolean enableAutocomplete;
    public final boolean includeMeta;
    public final boolean hasApproval;
    public final Set<Field> fields;
    public final ImmutableResult meta;
    public final Set<String> srcExcludes;
    public final Map<String, List<String>> relations;

    @Override public boolean equals(Object o) {
        return this == o || (!(o == null || getClass() != o.getClass()) && mappingJson().equals(((Type) o).mappingJson()));
    }

    @Override public int hashCode() { return 29 * mappingJson().hashCode(); }

    public boolean hasRelations() { return relations.size() > 0; }

    public String mappingJson() { return serializeType(this); }

    List<String> sourceExcludes() {
        return ListBuilder.<String>immutableListBuilder().addAll(srcExcludes)
            .addAll(transformColl(filterColl(fields, Field::excludeFromSrc), Field::field)).build();
    }

    public static Builder typeBuilder() { return new Builder().exclude(AUTO_COMPLETE_FIELD); }

    @Setter @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private boolean strict = false;
        private boolean enabled = true;
        private boolean enableSource = true;
        private boolean enableSize = true;
        private boolean enableAll = true;
        private boolean enableAutocomplete = true;
        private boolean includeMeta = true;
        private boolean hasApproval = false;
        private final Set<Field> fields = new TreeSet<>();
        private final ImmutableMapBuilder<String, Object> metaData = immutableResultBuilder();
        private final Set<String> srcExcludes = new TreeSet<>();
        private final ListMultimap<String, String> relations = ArrayListMultimap.create();

        public Builder strict() { return strict(true); }

        public Builder field(FieldBuilder builder) { return field(builder.build()); }

        public Builder field(Field field) { return fields(singleton(field)); }

        public Builder fields(Collection<Field> fields) { this.fields.removeAll(fields); this.fields.addAll(fields); return this; }

        public Builder exclude(String excl) { this.srcExcludes.add(excl); return this; }

        public Builder metaData(String name, Object value) { metaData.put(name, value); return this; }

        public Builder relation(String parent, String child) { relations.put(parent, child); return this; }

        public Builder relation(String parent, Iterable<String> children) { relations.putAll(parent, children); return this; }

        public Builder with(Consumer<Builder> consumer) { consumer.accept(this); return this; }

        public Type build() {
            return new Type(strict, enabled, enableSource, enableSize, enableAll, enableAutocomplete, includeMeta, hasApproval,
                immutableSet(fields), metaData.immutable(), immutableSet(srcExcludes), immutableMap(asMap(relations)));
        }
    }
}