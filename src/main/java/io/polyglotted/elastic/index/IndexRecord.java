package io.polyglotted.elastic.index;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.model.MapResult.ImmutableResult;
import io.polyglotted.common.model.SortedMapResult;
import io.polyglotted.elastic.common.DocStatus;
import io.polyglotted.elastic.common.MetaFields;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.elasticsearch.action.DocWriteRequest;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Set;

import static io.polyglotted.common.model.SortedMapResult.treeResult;
import static io.polyglotted.common.util.BaseSerializer.serializeBytes;
import static io.polyglotted.common.util.CollUtil.filterKeys;
import static io.polyglotted.common.util.MapBuilder.immutableMap;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.common.util.StrUtil.notNullOrEmpty;
import static io.polyglotted.common.util.UrnUtil.urnOf;
import static io.polyglotted.common.util.UuidUtil.generateUuid;
import static io.polyglotted.elastic.common.MetaFields.ANCESTOR_FIELD;
import static io.polyglotted.elastic.common.MetaFields.APPROVAL_ROLES_FIELD;
import static io.polyglotted.elastic.common.MetaFields.BASE_TS_FIELD;
import static io.polyglotted.elastic.common.MetaFields.COMMENT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.EXPIRY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.KEY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.MODEL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.PARENT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.SCHEMA_FIELD;
import static io.polyglotted.elastic.common.MetaFields.STATUS_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TIMESTAMP_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TRAITFQN_FIELD;
import static io.polyglotted.elastic.common.MetaFields.UPDATER_FIELD;
import static io.polyglotted.elastic.common.MetaFields.USER_FIELD;
import static io.polyglotted.elastic.common.MetaFields.addMeta;
import static io.polyglotted.elastic.common.MetaFields.removeMeta;
import static io.polyglotted.elastic.index.RecordAction.CREATE;
import static io.polyglotted.elastic.index.RecordAction.DELETE;
import static io.polyglotted.elastic.index.RecordAction.UPDATE;
import static java.util.Objects.requireNonNull;
import static lombok.AccessLevel.PRIVATE;

@SuppressWarnings({"unused", "WeakerAccess", "UnusedReturnValue"})
@RequiredArgsConstructor(access = PRIVATE)
public final class IndexRecord {
    public final String index;
    public final String model;
    public final String id;
    public final String parent;
    public final long timestamp;
    public final RecordAction action;
    public final Long baseVersion;
    public final Object source;
    public final ImmutableResult ancillary;
    public final String pipeline;
    private String ancestorId = null;
    @Getter private String result = null;

    public String keyString() { return urnOf(model, id); }

    public String simpleKey() { return MetaFields.simpleKey(model, parent, id, timestamp); }

    void update(String ancestorId, String ancestor, DocStatus status) {
        this.ancestorId = ancestorId; this.result = status.toString(); addMeta(source, ANCESTOR_FIELD, ancestor);
    }

    String ancestorId() { return requireNonNull(ancestorId, "_id not found for delete"); }

    DocWriteRequest<?> request() { System.out.println("adding key " + simpleKey()); addMeta(source, KEY_FIELD, simpleKey()); return action.request(this); }

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
        private final SortedMapResult ancillary = treeResult();
        private Long timestamp = null;
        private Long baseVersion = null;
        @Setter private String pipeline = null;

        private Builder(RecordAction action, String index, String modelName, String idStr, String parent, Object object) {
            this.index = requireNonNull(index);
            this.model = requireNonNull(modelName);
            byte[] bytes = getBytes(object);
            this.id = nonNull(idStr, () -> generateUuid(bytes).toString().toLowerCase());
            this.parent = parent; // can be null
            this.action = requireNonNull(action);
            this.keyString = urnOf(model, id);
            this.source = object;
            addMeta(source, ID_FIELD, id);
            if (notNullOrEmpty(parent)) {
                addMeta(source, MODEL_FIELD, immutableMap("name", model, "parent", parent));
                addMeta(source, PARENT_FIELD, parent);
            }
            else { addMeta(source, MODEL_FIELD, model); }
        }

        @Override
        public boolean equals(Object o) {
            return this == o || (!(o == null || getClass() != o.getClass()) && Objects.equals(keyString, ((Builder) o).keyString));
        }

        @Override
        public int hashCode() { return Objects.hash(keyString, action); }

        public Builder timestamp(long timestampVal) {
            this.timestamp = timestampVal; addMeta(source, TIMESTAMP_FIELD, String.valueOf(timestamp));
            ancillary.put(EXPIRY_FIELD, String.valueOf(timestamp)); return this;
        }

        public Builder user(String user) {
            addMeta(source, USER_FIELD, requireNonNull(user)); ancillary.put(UPDATER_FIELD, user); return this;
        }

        public Builder comment(String comment, boolean meta) {
            if (meta) { addMeta(source, COMMENT_FIELD, requireNonNull(comment)); }
            else { ancillary.put(COMMENT_FIELD, requireNonNull(comment)); } return this;
        }

        public Builder traitFqn(String traitFqn) { addMeta(source, TRAITFQN_FIELD, traitFqn); return this; }

        public Builder schema(String schema) { addMeta(source, SCHEMA_FIELD, schema); return this; }

        public Builder baseVersion(Long baseVersion) { this.baseVersion = baseVersion; return this; }

        public Builder baseTimestamp(Long baseTs) { if (baseTs != null) { addMeta(source, BASE_TS_FIELD, baseTs); } return this; }

        public Builder status(DocStatus status) { addMeta(source, STATUS_FIELD, status.name()); return this; }

        public Builder noStatus() { removeMeta(source, STATUS_FIELD); removeMeta(source, BASE_TS_FIELD); return this; }

        public Builder approvalRoles(Set<String> roles) { if (notEmpty(roles)) { addMeta(source, APPROVAL_ROLES_FIELD, roles); } return this; }

        public IndexRecord build() {
            return new IndexRecord(index, model, id, parent, requireNonNull(timestamp), action, baseVersion, source, ancillary.immutable(), pipeline);
        }
    }

    private static byte[] getBytes(Object v) {
        return (v instanceof MapResult) ? serializeBytes(filterKeys((MapResult) v, MetaFields::isNotMeta)) : serializeBytes(v);
    }

    private static <T> boolean notEmpty(Collection<T> nullable) { return nullable != null && !nullable.isEmpty(); }
}