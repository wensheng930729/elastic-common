package io.polyglotted.elastic.common;

import io.polyglotted.common.model.MapResult;

import static io.polyglotted.common.model.MapResult.immutableResult;
import static io.polyglotted.common.util.MapRetriever.reqdStr;
import static io.polyglotted.common.util.ReflectionUtil.fieldValue;
import static io.polyglotted.elastic.common.MetaFields.ALL_FIELDS;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.KEY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.LINK_FIELD;
import static io.polyglotted.elastic.common.MetaFields.MODEL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.PARENT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TIMESTAMP_FIELD;
import static io.polyglotted.elastic.common.MetaFields.readHeader;
import static io.polyglotted.elastic.common.MetaFields.readKey;

@SuppressWarnings("unused")
public enum Verbose {
    NONE() {
        @Override public <T> T buildFrom(MapResult source, T result) { return result; }
    },
    KEY(KEY_FIELD) {
        @Override public <T> T buildFrom(MapResult source, T result) { return addHeader(result, readPlainKey(source)); }
    },
    MINIMAL(LINK_FIELD, MODEL_FIELD, PARENT_FIELD, ID_FIELD, TIMESTAMP_FIELD) {
        @Override public <T> T buildFrom(MapResult source, T result) { return addHeader(result, readKey(source).result()); }
    },
    PARENT(PARENT_FIELD) {
        @Override public <T> T buildFrom(MapResult source, T result) { return addHeader(result, readParent(source)); }
    },
    UNIQUE(ID_FIELD, KEY_FIELD) {
        @Override public <T> T buildFrom(MapResult source, T result) { return addHeader(result, readUnique(source)); }
    },
    META(ALL_FIELDS) {
        @Override public <T> T buildFrom(MapResult source, T result) { return addHeader(result, readHeader(source)); }
    };

    public final String[] fields;

    Verbose(String... array) { this.fields = array; }

    public abstract <T> T buildFrom(MapResult source, T result);

    public String[] includeFields(String[] includeFields) {
        if (includeFields.length <= 0) { return includeFields; }
        String[] result = new String[fields.length + includeFields.length];
        System.arraycopy(fields, 0, result, 0, fields.length);
        System.arraycopy(includeFields, 0, result, fields.length, includeFields.length);
        return result;
    }

    public static String param(Verbose verbose) { return verbose == null || verbose == NONE ? null : verbose.toString(); }

    public static Verbose toMeta(String verb) { return verb == null ? NONE : META; }

    public static Verbose fromVerb(String verb) { return verb == null ? NONE : verb.isEmpty() ? META : Verbose.valueOf(verb.toUpperCase()); }

    @SuppressWarnings("unchecked")
    private static <T> T addHeader(T result, MapResult header) {
        if (result instanceof MapResult) { ((MapResult) result).putAll(header); }
        else { fieldValue(result, "_meta", header); }
        return result;
    }

    private static MapResult readParent(MapResult map) { return immutableResult(PARENT_FIELD, reqdStr(map, PARENT_FIELD)); }

    private static MapResult readPlainKey(MapResult map) { return immutableResult(KEY_FIELD, reqdStr(map, KEY_FIELD)); }

    private static MapResult readUnique(MapResult map) {
        return immutableResult(ID_FIELD, reqdStr(map, ID_FIELD), KEY_FIELD, reqdStr(map, KEY_FIELD));
    }
}