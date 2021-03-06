package io.polyglotted.elastic.common;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.MapBuilder;
import io.polyglotted.common.util.MapBuilder.ImmutableMapBuilder;
import io.polyglotted.common.util.MapBuilder.SimpleMapBuilder;

import java.util.List;
import java.util.TreeMap;

import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.util.Assertions.checkContains;
import static io.polyglotted.common.util.CollUtil.filterKeys;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.common.util.MapBuilder.immutableMapBuilder;
import static io.polyglotted.common.util.MapBuilder.simpleMapBuilder;
import static io.polyglotted.common.util.MapRetriever.longStrVal;
import static io.polyglotted.common.util.MapRetriever.optValue;
import static io.polyglotted.common.util.MapRetriever.reqdValue;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.common.util.ReflectionUtil.safeFieldValue;
import static io.polyglotted.common.util.UrnUtil.urnOf;
import static io.polyglotted.elastic.common.DocStatus.fromStatus;

@SuppressWarnings({"unused", "unchecked", "WeakerAccess"})
public abstract class MetaFields {
    public static final String ALL_FIELD = "&all";
    public static final String ANCESTOR_FIELD = "&ancestor";
    public static final String APPROVAL_ROLES_FIELD = "&approvalRoles";
    public static final String AUTO_COMPLETE_FIELD = "&autoComplete";
    public static final String COMMENT_FIELD = "&comment";
    public static final String EXPIRY_FIELD = "&expiry";
    public static final String HIGHLTGHT_FIELD = "&highlight";
    public static final String ID_FIELD = "&id";
    public static final String KEY_FIELD = "&key";
    public static final String LINK_FIELD = "&link";
    public static final String MODEL_FIELD = "&model";
    public static final String PARENT_FIELD = "&parent";
    public static final String REALM_FIELD = "&realm";
    public static final String RESULT_FIELD = "&result";
    public static final String STATUS_FIELD = "&status";
    public static final String TIMESTAMP_FIELD = "&timestamp";
    public static final String TRAITFQN_FIELD = "&traitFqn";
    public static final String UNIQUE_FIELD = "&uniqueProps";
    public static final String UPDATER_FIELD = "&updater";
    public static final String USER_FIELD = "&user";

    private static final List<String> KEY_FIELDS = immutableList(MODEL_FIELD, ID_FIELD, TIMESTAMP_FIELD, KEY_FIELD, STATUS_FIELD, EXPIRY_FIELD);
    public static final List<String> ALL_FIELDS = immutableList(ANCESTOR_FIELD, APPROVAL_ROLES_FIELD, COMMENT_FIELD, EXPIRY_FIELD, ID_FIELD,
        KEY_FIELD, LINK_FIELD, MODEL_FIELD, PARENT_FIELD, REALM_FIELD, STATUS_FIELD, TRAITFQN_FIELD, TIMESTAMP_FIELD, UPDATER_FIELD, USER_FIELD);

    public static void addMeta(Object item, String field, Object value) { addMetaField(mapValue(item), field, value); }

    public static void addMetaField(MapResult mapValue, String field, Object value) { mapValue.put(field, value); }

    public static void removeMeta(Object item, String field) { mapValue(item).remove(field); }

    public static <T> T reqdMeta(Object object, String field) { return reqdValue(mapValue(object), field); }

    public static boolean isMeta(String field) { return field.indexOf('&') == 0; }

    public static boolean isNotMeta(String field) { return field.indexOf('&') != 0; }

    public static String model(Object object) { return reqdMeta(object, MODEL_FIELD); }

    public static String reqdId(Object object) { return reqdMeta(object, ID_FIELD); }

    public static String reqdKey(Object object) { return reqdMeta(object, KEY_FIELD); }

    public static String id(Object object) { return optValue(mapValue(object), ID_FIELD); }

    public static String parent(Object object) { return optValue(mapValue(object), PARENT_FIELD); }

    public static long timestamp(Object object) { return longStrVal(mapValue(object), TIMESTAMP_FIELD, -3); }

    public static Long tstamp(Object object) { return longStrVal(mapValue(object), TIMESTAMP_FIELD); }

    public static String keyString(MapResult map) { return urnOf(model(map), id(map)); }

    public static DocStatus status(MapResult map) { return fromStatus(map.get(STATUS_FIELD).toString()); }

    public static ImmutableMapBuilder<String, Object> readKey(MapResult map) {
        ImmutableMapBuilder<String, Object> builder = immutableMapBuilder();
        putVal(map, checkContains(map, ID_FIELD), builder);
        putVal(map, checkContains(map, KEY_FIELD), builder);
        putVal(map, LINK_FIELD, builder);
        putVal(map, checkContains(map, MODEL_FIELD), builder);
        putVal(map, PARENT_FIELD, builder);
        putTs(map, checkContains(map, TIMESTAMP_FIELD), builder);
        return builder;
    }

    public static MapResult readHeader(MapResult map) { return readHeader(map, true); }

    public static MapResult readHeader(MapResult map, boolean mandatory) {
        SimpleMapBuilder<String, Object> builder = simpleMapBuilder(TreeMap::new);
        putVal(map, mandatory ? checkContains(map, MODEL_FIELD) : MODEL_FIELD, builder);
        putVal(map, mandatory ? checkContains(map, ID_FIELD) : ID_FIELD, builder);
        putTs(map, mandatory ? checkContains(map, TIMESTAMP_FIELD) : TIMESTAMP_FIELD, builder);
        putVal(map, mandatory ? checkContains(map, KEY_FIELD) : KEY_FIELD, builder);
        if (map.containsKey(STATUS_FIELD)) { builder.put(STATUS_FIELD, status(map));}
        putTs(map, EXPIRY_FIELD, builder);
        filterKeys(map, MetaFields::isMeta).forEach((key, value) -> { if (!KEY_FIELDS.contains(key)) { builder.put(key, value); }});
        return builder.immutable();
    }

    private static void putTs(MapResult map, String property, MapBuilder builder) {
        if (map.containsKey(property)) builder.put(property, longStrVal(map, property, -3));
    }

    private static void putVal(MapResult map, String property, MapBuilder builder) {
        if (map.containsKey(property)) builder.put(property, map.get(property));
    }

    private static MapResult mapValue(Object item) {
        return (item instanceof MapResult) ? (MapResult) item : nonNull(safeFieldValue(item, "_meta"), simpleResult());
    }
}