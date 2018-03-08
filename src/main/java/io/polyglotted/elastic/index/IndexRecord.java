package io.polyglotted.elastic.index;

import com.google.common.annotations.VisibleForTesting;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.model.MapResult.TreeMapResult;
import io.polyglotted.elastic.common.DocStatus;
import io.polyglotted.elastic.common.MetaFields;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.elasticsearch.action.DocWriteRequest;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.filterKeys;
import static io.polyglotted.common.util.BaseSerializer.serializeBytes;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.common.util.StrUtil.notNullOrEmpty;
import static io.polyglotted.common.util.UrnUtil.safeUrnOf;
import static io.polyglotted.common.util.UrnUtil.urnOf;
import static io.polyglotted.common.util.UuidUtil.generateUuid;
import static io.polyglotted.elastic.common.MetaFields.APPROVAL_ROLES_FIELD;
import static io.polyglotted.elastic.common.MetaFields.BASE_TS_FIELD;
import static io.polyglotted.elastic.common.MetaFields.COMMENT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.EXPIRY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.KEY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.MODEL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.PARENT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.SCHEMA_FIELD;
import static io.polyglotted.elastic.common.MetaFields.SERIES_REFFQN_FIELD;
import static io.polyglotted.elastic.common.MetaFields.STATUS_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TIMESTAMP_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TRAITFQN_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TRAITID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TTL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.UPDATER_FIELD;
import static io.polyglotted.elastic.common.MetaFields.USER_FIELD;
import static io.polyglotted.elastic.common.MetaFields.addMeta;
import static io.polyglotted.elastic.common.MetaFields.removeMeta;
import static io.polyglotted.elastic.common.MetaFields.timestampStr;
import static io.polyglotted.elastic.index.RecordAction.CREATE;
import static io.polyglotted.elastic.index.RecordAction.DELETE;
import static io.polyglotted.elastic.index.RecordAction.UPDATE;

@ToString(includeFieldNames = false, doNotUseGetters = true)
@SuppressWarnings({"unused", "WeakerAccess", "UnusedReturnValue"})
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class IndexRecord {
    public final String index;
    public final String model;
    public final String id;
    public final String parent;
    public final RecordAction action;
    public final String keyString;
    public final Long baseVersion;
    public final Object source;
    private final MapResult ancillary;
    public final String pipeline;

    public long timestamp() { return MetaFields.timestamp(source); }

    public String key() { return safeUrnOf(model, parent, id, timestampStr(source)); }

    public String simpleKey() { return safeUrnOf(model, id, timestampStr(source)); }

    MapResult update(MapResult current) { current.putAll(ancillary); current.put(STATUS_FIELD, action.status); return current; }

    public DocWriteRequest<?> request() { addMeta(source, KEY_FIELD, key()); return action.request(this); }

    public static Builder createRecord(String repo, String model, String id, Object source) { return createRecord(repo, model, id, null, source); }

    public static Builder createRecord(String repo, String model, String id, String parent, Object source) {
        return new Builder(CREATE, repo, model, id, parent, source);
    }

    public static Builder updateRecord(String repo, String model, String id, Long version, Object source) {
        return updateRecord(repo, model, id, null, version, source);
    }

    public static Builder updateRecord(String repo, String model, String id, String parent, Long version, Object source) {
        return expired(UPDATE, repo, model, id, parent, version, source);
    }

    public static Builder deleteRecord(String repo, String model, String id, Long version) { return deleteRecord(repo, model, id, null, version); }

    public static Builder deleteRecord(String repo, String model, String id, String parent, Long version) {
        return expired(DELETE, repo, model, id, parent, version, new LinkedHashMap<>());
    }

    @VisibleForTesting
    public static Builder expired(RecordAction action, String repo, String model, String id, String parent, Long tstamp, Object src) {
        return new Builder(action, repo, model, id, parent, src).baseVersion(tstamp);
    }

    @Accessors(fluent = true, chain = true)
    public static class Builder {
        public final String index;
        public final String model;
        public final String id;
        public final String parent;
        private final RecordAction action;
        @Getter final String keyString;
        @Getter private final Object source;
        private final MapResult ancillary = new TreeMapResult();
        private Long baseVersion = null;
        @Setter private String pipeline = null;

        private Builder(RecordAction action, String index, String modelName, String idStr, String parent, Object object) {
            this.index = checkNotNull(index);
            this.model = checkNotNull(modelName);
            this.id = nonNull(idStr, genUuid(object));
            this.parent = parent; // can be null
            this.action = checkNotNull(action);
            this.keyString = urnOf(model, id);
            this.source = object;
            addMeta(source, MODEL_FIELD, model); addMeta(source, ID_FIELD, id);
            if (notNullOrEmpty(parent)) { addMeta(source, PARENT_FIELD, parent); }
        }

        @Override
        public boolean equals(Object o) {
            return this == o || (!(o == null || getClass() != o.getClass()) && Objects.equals(keyString, ((Builder) o).keyString));
        }

        @Override
        public int hashCode() { return Objects.hash(keyString, action); }

        public Builder timestamp(Long timestamp) {
            addMeta(source, TIMESTAMP_FIELD, checkNotNull(timestamp).toString()); ancillary.put(EXPIRY_FIELD, timestamp.toString()); return this;
        }

        public Builder user(String user) { addMeta(source, USER_FIELD, checkNotNull(user)); ancillary.put(UPDATER_FIELD, user); return this; }

        public Builder comment(String comment, boolean meta) {
            if (meta) { addMeta(source, COMMENT_FIELD, checkNotNull(comment)); }
            else { ancillary.put(COMMENT_FIELD, checkNotNull(comment)); } return this;
        }

        public Builder traitFqn(String traitFqn) { addMeta(source, TRAITFQN_FIELD, traitFqn); return this; }

        public Builder traitId(String traitId) { addMeta(source, TRAITID_FIELD, checkNotNull(traitId)); return this; }

        public Builder schema(String schema) { addMeta(source, SCHEMA_FIELD, schema); return this; }

        public Builder seriesRefFqn(String refFqn) { if (refFqn != null) { addMeta(source, SERIES_REFFQN_FIELD, refFqn); } return this; }

        public Builder baseVersion(Long baseVersion) { this.baseVersion = baseVersion; return this; }

        public Builder baseTimestamp(Long baseTs) { if (baseTs != null) { addMeta(source, BASE_TS_FIELD, baseTs); } return this; }

        public Builder status(DocStatus status) { addMeta(source, STATUS_FIELD, status.name()); return this; }

        public Builder noStatus() { removeMeta(source, STATUS_FIELD); removeMeta(source, BASE_TS_FIELD); return this; }

        public Builder approvalRoles(Set<String> roles) { if (notEmpty(roles)) { addMeta(source, APPROVAL_ROLES_FIELD, roles); } return this; }

        public Builder ttlExpiry(long ttl) { addMeta(source, TTL_FIELD, ttl); return this; }

        public IndexRecord build() { return new IndexRecord(index, model, id, parent, action, keyString, baseVersion, source, ancillary, pipeline); }
    }

    private static String genUuid(Object v) {
        byte[] nameBytes = (v instanceof MapResult) ? serializeBytes(filterKeys((MapResult) v, MetaFields::isNotMeta)) : serializeBytes(v);
        return generateUuid(nameBytes).toString().toLowerCase();
    }

    private static <T> boolean notEmpty(Collection<T> nullable) { return nullable != null && !nullable.isEmpty(); }
}