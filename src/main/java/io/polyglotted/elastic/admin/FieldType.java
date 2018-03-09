package io.polyglotted.elastic.admin;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.EnumSet;

import static com.google.common.base.Preconditions.checkArgument;

@SuppressWarnings({"unused", "WeakerAccess"})
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
    OBJECT(false, false, false);

    private final boolean docValues;
    private final boolean storable;
    private final boolean indexable;

    private static final EnumSet<FieldType> SIMPLE_FIELDS = EnumSet.of(BINARY, BOOLEAN, DATE, GEO_POINT, GEO_SHAPE, IP,
        DOUBLE, FLOAT, HALF_FLOAT, SCALED_FLOAT, BYTE, SHORT, INTEGER, LONG);

    FieldType simpleField() { checkArgument(SIMPLE_FIELDS.contains(this), name() + " not a simple type"); return this; }

    String validate(Field field) {
        checkArgument(docValues || field.docValues == null, "field " + field.field + " with type " + name() + " cannot contain docValues");
        checkArgument(storable || field.stored == null, "field " + field.field + " with type " + name() + " cannot be stored");
        checkArgument(indexable || field.indexed == null, "field " + field.field + " with type " + name() + " cannot be indexed");
        return name().toLowerCase();
    }
}