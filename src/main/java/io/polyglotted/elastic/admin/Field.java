package io.polyglotted.elastic.admin;

import io.polyglotted.common.model.MapResult.ImmutableResult;
import io.polyglotted.common.model.SortedMapResult;
import io.polyglotted.common.util.ListBuilder.ImmutableListBuilder;
import io.polyglotted.common.util.MapBuilder.ImmutableMapBuilder;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.polyglotted.common.model.SortedMapResult.treeResult;
import static io.polyglotted.common.util.BaseSerializer.serialize;
import static io.polyglotted.common.util.CollUtil.transform;
import static io.polyglotted.common.util.CollUtil.uniqueIndex;
import static io.polyglotted.common.util.ListBuilder.immutableListBuilder;
import static io.polyglotted.common.util.MapBuilder.immutableMapBuilder;
import static io.polyglotted.common.util.MapRetriever.reqdStr;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.elastic.admin.FieldType.KEYWORD;
import static io.polyglotted.elastic.admin.FieldType.NESTED;
import static io.polyglotted.elastic.admin.FieldType.OBJECT;
import static io.polyglotted.elastic.admin.FieldType.TEXT;

@SuppressWarnings({"unused", "WeakerAccess"})
@ToString(includeFieldNames = false, doNotUseGetters = true)
@Accessors(fluent = true) @RequiredArgsConstructor
public final class Field implements Comparable<Field> {
    @Getter public final String field;
    public final FieldType type;
    public final String analyzer;
    public final Boolean docValues;
    public final Boolean indexed;
    public final Boolean stored;
    public final Boolean hasFields;
    public final ImmutableResult argsMap;
    public final List<String> copyToFields;
    public final Map<String, Field> properties;
    @Getter public final boolean excludeFromSrc;

    @Override
    public boolean equals(Object o) { return this == o || o != null && getClass() == o.getClass() && serialize(this).equals(serialize(o)); }

    @Override public int hashCode() { return field.hashCode(); }

    @Override public int compareTo(Field other) { return (other == null) ? -1 : field.compareTo(other.field); }

    boolean hasProperties() { return (type == NESTED || type == OBJECT) && properties.size() > 0; }

    boolean hasFields() { return Boolean.TRUE.equals(hasFields) && properties.size() > 0; }

    public static FieldBuilder keywordField(String field) { return fieldBuilder(field, KEYWORD); }

    public static FieldBuilder nestedField(String field) { return fieldBuilder(field, NESTED); }

    public static FieldBuilder objectField(String field) { return fieldBuilder(field, OBJECT); }

    public static FieldBuilder nonIndexField(String field, FieldType fieldType) { return fieldBuilder(field, fieldType).indexed(false); }

    public static FieldBuilder textField(String field, String analyzer) { return fieldBuilder(field, TEXT).analyzer(nonNull(analyzer, "standard")); }

    public static FieldBuilder simpleField(String field, FieldType fieldType) { return fieldBuilder(field, fieldType.simpleField()); }

    private static FieldBuilder fieldBuilder(String field, FieldType fieldType) { return new FieldBuilder(field, fieldType); }

    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class FieldBuilder {
        @NonNull @Getter private final String field;
        @NonNull @Getter private final FieldType type;
        @Setter(AccessLevel.PRIVATE) private String analyzer = null;
        @Setter private Boolean docValues = null;
        @Setter private Boolean indexed = null;
        @Setter private Boolean stored = null;
        @Setter(AccessLevel.PRIVATE) private Boolean hasFields = null;
        private final SortedMapResult args = treeResult();
        private final ImmutableListBuilder<String> copyToFields = immutableListBuilder();
        private final ImmutableMapBuilder<String, FieldBuilder> properties = immutableMapBuilder();
        @Setter private boolean excludeFromSrc = false;

        public FieldBuilder mapping(FieldBuilder mapping) { this.properties.put(mapping.field, mapping); return this; }

        public FieldBuilder properties(Collection<FieldBuilder> builders) { builders.forEach(this::mapping); return this; }

        public FieldBuilder properties(Map<String, FieldBuilder> builders) { this.properties.putAll(builders); return this; }

        public FieldBuilder isAPath() { return addField("tree", "pathAnalyzer"); }

        public FieldBuilder normalise() { return addField("text", "normAnalyzer"); }

        public FieldBuilder addRawFields() { hasFields(true); return mapping(keywordField("raw")); }

        public FieldBuilder addField(String field, String analyzer) { hasFields(true); return mapping(textField(field, analyzer)); }

        public FieldBuilder addField(String field, Map<String, Object> props) {
            FieldBuilder subField = textField(field, reqdStr(props, "analyzer"));
            props.forEach((k, v) -> { if (!"analyzer".equals(k)) { subField.args.put(k, v); }});
            hasFields(true); return mapping(subField);
        }

        public FieldBuilder dynamic(Object value) { if (value != null) { this.args.put("dynamic", value); } return this; }

        public FieldBuilder scaling(double value) { this.args.put("scaling_factor", value); return this; }

        public FieldBuilder format(String value) { if (value != null) { this.args.put("format", value); } return this; }

        public FieldBuilder quadtree() { this.args.put("tree", "quadtree"); return this; }

        public FieldBuilder disabled() { this.args.put("enabled", false); return this; }

        public FieldBuilder disableOrDynamic(boolean disable) { return disable ? disabled() : dynamic("true"); }

        public FieldBuilder extra(String name, Object value) { if (value != null) { this.args.put(name, value); } return this; }

        public FieldBuilder copyTo(String value) { if (value != null) { this.copyToFields.add(value); } return this; }

        public FieldBuilder copyToFields(Collection<String> copyFields) { this.copyToFields.addAll(copyFields); return this; }

        public FieldBuilder args(Map<String, Object> args) { this.args.putAll(args); return this; }

        public FieldBuilder duplicate(String newField) {
            return fieldBuilder(newField, this.type).analyzer(this.analyzer).docValues(this.docValues).indexed(this.indexed)
                .stored(this.stored).hasFields(this.hasFields).copyToFields(this.copyToFields.build()).args(this.args)
                .properties(this.properties.build());
        }

        public Field build() {
            return new Field(field, type, analyzer, docValues, indexed, stored, hasFields, args.immutable(), copyToFields.build(),
                uniqueIndex(transform(properties.build().values(), FieldBuilder::build), Field::field), excludeFromSrc);
        }
    }
}