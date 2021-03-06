package io.polyglotted.elastic.admin;

import io.polyglotted.common.util.MapBuilder.ImmutableMapBuilder;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.EnumSet;
import java.util.Map;

import static io.polyglotted.common.util.Assertions.checkBool;
import static io.polyglotted.common.util.Assertions.checkContains;
import static io.polyglotted.common.util.MapBuilder.immutableMapBuilder;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum FieldType {
    BINARY(true, true, false),
    BOOLEAN(true, true, true),
    DATE(true, true, true),
    GEO_POINT(false, false, false),
    GEO_SHAPE(false, false, false),
    IP(true, true, true),
    DOUBLE(true, true, true),
    FLOAT(true, true, true),
    HALF_FLOAT(true, true, true),
    SCALED_FLOAT(true, true, true),
    BYTE(true, true, true),
    SHORT(true, true, true),
    INTEGER(true, true, true),
    LONG(true, true, true),
    KEYWORD(true, true, true),
    TEXT(false, true, true),
    NESTED(false, false, false),
    OBJECT(false, false, false),
    JOIN(false, false, false);

    private final boolean docValues;
    private final boolean storable;
    private final boolean indexable;

    private static final EnumSet<FieldType> SIMPLE_FIELDS = EnumSet.of(BINARY, BOOLEAN, DATE, GEO_POINT, GEO_SHAPE, IP,
        DOUBLE, FLOAT, HALF_FLOAT, SCALED_FLOAT, BYTE, SHORT, INTEGER, LONG, KEYWORD);
    private final static Map<String, FieldType> TYPES;

    static {
        ImmutableMapBuilder<String, FieldType> builder = immutableMapBuilder();
        for (FieldType type : values()) { builder.put(type.name(), type).put(type.name().toLowerCase(), type); }
        TYPES = builder.build();
    }

    @SuppressWarnings("unused") public static FieldType fieldType(String value) { return TYPES.get(value); }

    FieldType simpleField() { return checkContains(SIMPLE_FIELDS, this, name() + " not a simple type"); }

    String validate(Field field) {
        checkBool(docValues || field.docValues == null, "field " + field.field + " with type " + name() + " cannot contain docValues");
        checkBool(storable || field.stored == null, "field " + field.field + " with type " + name() + " cannot be stored");
        checkBool(indexable || field.indexed == null, "field " + field.field + " with type " + name() + " cannot be indexed");
        return name().toLowerCase();
    }
}