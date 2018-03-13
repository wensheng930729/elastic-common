package io.polyglotted.elastic.admin;

import com.google.common.collect.ImmutableSet;
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
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.ImmutableSet.copyOf;
import static io.polyglotted.common.model.MapResult.immutableResultBuilder;
import static io.polyglotted.elastic.admin.TypeSerializer.serializeType;
import static io.polyglotted.elastic.common.MetaFields.ALL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.AUTO_COMPLETE_FIELD;
import static io.polyglotted.elastic.common.MetaFields.UNIQUE_FIELD;
import static java.util.Collections.singleton;

@ToString(includeFieldNames = false, doNotUseGetters = true)
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class Type {
    public final String type = "_doc";
    public final String parent;
    public final boolean strict;
    public final boolean enabled;
    public final boolean enableSource;
    public final boolean includeMeta;
    public final boolean excludeUniqueProps;
    public final ImmutableSet<Field> fields;
    public final ImmutableResult meta;

    @Override public boolean equals(Object o) {
        return this == o || (!(o == null || getClass() != o.getClass()) && mappingJson().equals(((Type) o).mappingJson()));
    }

    @Override public int hashCode() { return 29 * mappingJson().hashCode(); }

    String mappingJson() { return serializeType(this); }

    @SuppressWarnings("StaticPseudoFunctionalStyleMethod") List<String> sourceExcludes() {
        return ListBuilder.<String>immutableListBuilder().add(ALL_FIELD).add(AUTO_COMPLETE_FIELD).add(excludeUniqueProps ? UNIQUE_FIELD : null)
            .addAll(transform(filter(fields, Field::excludeFromSrc), Field::field)).build();
    }

    public static Builder typeBuilder() { return new Builder(); }

    @Setter @Accessors(fluent = true, chain = true)
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String parent;
        private boolean strict = false;
        private boolean enabled = true;
        private boolean enableSource = true;
        private boolean includeMeta = true;
        private boolean excludeUniqueProps = false;
        private final Set<Field> fields = new TreeSet<>();
        private final ImmutableMapBuilder<String, Object> metaData = immutableResultBuilder();

        public Builder strict() { return strict(true); }

        public Builder excludeUniqueProps() { return excludeUniqueProps(true); }

        public Builder field(FieldBuilder builder) { return field(builder.build()); }

        public Builder field(Field field) { return fields(singleton(field)); }

        public Builder fields(Collection<Field> fields) { this.fields.removeAll(fields); this.fields.addAll(fields); return this; }

        public Builder metaData(String name, Object value) { if (value != null) { metaData.put(name, value); } return this; }

        public Type build() {
            return new Type(parent, strict, enabled, enableSource, includeMeta, excludeUniqueProps, copyOf(fields), metaData.immutable());
        }
    }
}