package io.polyglotted.elastic.common;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.MapBuilder.ImmutableMapBuilder;

import java.util.List;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static io.polyglotted.common.model.MapResult.immutableResultBuilder;
import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.common.util.MapBuilder.immutableMapBuilder;
import static io.polyglotted.common.util.MapRetriever.listVal;
import static io.polyglotted.common.util.MapRetriever.longStrVal;
import static io.polyglotted.common.util.MapRetriever.optStr;
import static io.polyglotted.common.util.MapRetriever.optValue;
import static io.polyglotted.common.util.MapRetriever.reqdStr;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.common.util.ReflectionUtil.safeFieldValue;
import static io.polyglotted.common.util.UrnUtil.safeUrnOf;
import static io.polyglotted.common.util.UrnUtil.urnOf;
import static io.polyglotted.common.util.UuidUtil.genUuidStr;

@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class MetaFields {
    public static final String ALL_FIELD = "&all";
    public static final String ANCESTOR_FIELD = "&ancestor";
    public static final String APPROVAL_ROLES_FIELD = "&approvalRoles";
    public static final String AUTO_COMPLETE_FIELD = "&autoComplete";
    public static final String BASE_TS_FIELD = "&baseTimestamp";
    public static final String COMMENT_FIELD = "&comment";
    public static final String EXPIRY_FIELD = "&expiry";
    public static final String INDEX_FIELD = "&index";
    public static final String ID_FIELD = "&id";
    public static final String KEY_FIELD = "&key";
    public static final String LINK_FIELD = "&link";
    public static final String MODEL_FIELD = "&model";
    public static final String PARENT_FIELD = "&parent";
    public static final String RESULT_FIELD = "&result";
    public static final String SCHEMA_FIELD = "&schema";
    public static final String SERIES_REFFQN_FIELD = "&seriesRefFqn";
    public static final String STATUS_FIELD = "&status";
    public static final String TIMESTAMP_FIELD = "&timestamp";
    public static final String TRAITFQN_FIELD = "&traitFqn";
    public static final String TRAITID_FIELD = "&traitId";
    public static final String TTL_FIELD = "&ttlExpiry";
    public static final String UNIQUE_FIELD = "&uniqueProps";
    public static final String UPDATER_FIELD = "&updater";
    public static final String USER_FIELD = "&user";

    public static final String[] ALL_FIELDS = immutableList(ANCESTOR_FIELD, APPROVAL_ROLES_FIELD, BASE_TS_FIELD, COMMENT_FIELD, EXPIRY_FIELD,
        INDEX_FIELD, ID_FIELD, KEY_FIELD, LINK_FIELD, MODEL_FIELD, PARENT_FIELD, SERIES_REFFQN_FIELD, STATUS_FIELD, SCHEMA_FIELD,
        TRAITFQN_FIELD, TRAITID_FIELD, TIMESTAMP_FIELD, TTL_FIELD, UPDATER_FIELD, USER_FIELD).toArray(new String[0]);

    public static <T> T addMetas(T item, MapResult fields) {
        MapResult mapValue = mapValue(item);
        for (Entry<String, Object> e : fields.entrySet()) { addMetaField(mapValue, e.getKey(), e.getValue()); }
        return item;
    }

    public static void addMeta(Object item, String field, Object value) { addMetaField(mapValue(item), field, value); }

    public static void addMetaField(MapResult mapValue, String field, Object value) { if (value != null) { mapValue.put(field, value); } }

    public static void removeMeta(Object item, String field) { mapValue(item).remove(field); }

    public static String optMeta(Object object, String field) { return optStr(mapValue(object), field); }

    public static String reqdMeta(Object object, String field) { return reqdStr(mapValue(object), field); }

    public static boolean isNotMeta(String field) { return field.indexOf('&') != 0; }

    public static String index(Object object) { return reqdMeta(object, INDEX_FIELD); }

    public static String model(Object object) { return reqdMeta(object, MODEL_FIELD); }

    public static String reqdId(Object object) { return reqdMeta(object, ID_FIELD); }

    public static String reqdKey(Object object) { return reqdMeta(object, KEY_FIELD); }

    public static String id(Object object) { return optValue(mapValue(object), ID_FIELD); }

    public static String parent(Object object) { return optValue(mapValue(object), PARENT_FIELD); }

    public static long timestamp(Object object) { return longStrVal(mapValue(object), TIMESTAMP_FIELD, -3); }

    public static <T> T timestampStr(Object object) { return optValue(mapValue(object), TIMESTAMP_FIELD); }

    public static List<String> uniqueProps(Object object) { return listVal(mapValue(object), UNIQUE_FIELD); }

    public static String keyString(MapResult map) { return urnOf(model(map), id(map)); }

    public static String uniqueId(MapResult map) {
        return safeUrnOf(model(map), parent(map), nullToEmpty(id(map)), String.valueOf(timestamp(map)));
    }

    public static String traitId(String model, String id, long timestamp) { return genUuidStr(safeUrnOf(model, id, String.valueOf(timestamp))); }

    public static ImmutableMapBuilder<String, Object> readKey(MapResult map) {
        ImmutableMapBuilder<String, Object> builder = immutableMapBuilder();
        putVal(map, LINK_FIELD, builder);
        putVal(map, reqdProp(map, MODEL_FIELD), builder);
        putVal(map, PARENT_FIELD, builder);
        putVal(map, reqdProp(map, ID_FIELD), builder);
        putTs(map, reqdProp(map, TIMESTAMP_FIELD), builder);
        return builder;
    }

    public static MapResult readHeader(MapResult map, boolean mandatory) {
        ImmutableMapBuilder<String, Object> builder = immutableResultBuilder();
        putVal(map, ANCESTOR_FIELD, builder);
        putVal(map, APPROVAL_ROLES_FIELD, builder);
        putTs(map, BASE_TS_FIELD, builder);
        putVal(map, COMMENT_FIELD, builder);
        putTs(map, EXPIRY_FIELD, builder);
        putVal(map, INDEX_FIELD, builder);
        putVal(map, mandatory ? reqdProp(map, ID_FIELD) : ID_FIELD, builder);
        putVal(map, mandatory ? reqdProp(map, KEY_FIELD) : KEY_FIELD, builder);
        putVal(map, LINK_FIELD, builder);
        putVal(map, mandatory ? reqdProp(map, MODEL_FIELD) : MODEL_FIELD, builder);
        putVal(map, PARENT_FIELD, builder);
        putVal(map, SERIES_REFFQN_FIELD, builder);
        if (map.containsKey(STATUS_FIELD)) {
            builder.put(STATUS_FIELD, DocStatus.fromStatus(String.valueOf(map.get(STATUS_FIELD))));
        }
        putVal(map, SCHEMA_FIELD, builder);
        putVal(map, TRAITFQN_FIELD, builder);
        putVal(map, TRAITID_FIELD, builder);
        putTs(map, mandatory ? reqdProp(map, TIMESTAMP_FIELD) : TIMESTAMP_FIELD, builder);
        putVal(map, TTL_FIELD, builder);
        putVal(map, UPDATER_FIELD, builder);
        putVal(map, mandatory ? reqdProp(map, USER_FIELD) : USER_FIELD, builder);
        return builder.result();
    }

    private static void putTs(MapResult map, String property, ImmutableMapBuilder<String, Object> builder) {
        if (map.containsKey(property)) builder.put(property, longStrVal(map, property, -3));
    }

    private static void putVal(MapResult map, String property, ImmutableMapBuilder<String, Object> builder) {
        if (map.containsKey(property)) builder.put(property, map.get(property));
    }

    private static String reqdProp(MapResult map, String property) {
        checkArgument(map.containsKey(property), property + " is missing in the map"); return property;
    }

    private static MapResult mapValue(Object item) {
        return (item instanceof MapResult) ? (MapResult) item : nonNull(safeFieldValue(item, "_meta"), simpleResult());
    }
}