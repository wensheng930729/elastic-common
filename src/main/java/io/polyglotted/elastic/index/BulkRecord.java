package io.polyglotted.elastic.index;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.ListBuilder.ImmutableListBuilder;
import io.polyglotted.elastic.common.MetaFields;
import io.polyglotted.elastic.common.Notification;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.polyglotted.common.util.Assertions.checkBool;
import static io.polyglotted.common.util.ListBuilder.immutableListBuilder;
import static io.polyglotted.common.util.MapBuilder.simpleMap;
import static io.polyglotted.elastic.common.DocStatus.PENDING;
import static io.polyglotted.elastic.common.MetaFields.id;
import static io.polyglotted.elastic.common.MetaFields.tstamp;
import static io.polyglotted.elastic.common.Notification.notificationBuilder;
import static io.polyglotted.elastic.index.ApprovalUtil.approvalModel;
import static io.polyglotted.elastic.index.IndexRecord.createRecord;
import static io.polyglotted.elastic.index.IndexRecord.saveRecord;
import static java.util.Objects.requireNonNull;

@SuppressWarnings({"unused", "WeakerAccess", "StaticPseudoFunctionalStyleMethod"})
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class BulkRecord {
    public final String repo;
    public final String model;
    public final String parent;
    public final List<IndexRecord> records;
    public final IgnoreErrors ignoreErrors;
    public final Validator validator;
    private final Notification.Builder notification;
    public final Map<String, String> failures = simpleMap();

    public Notification notification() { return notification.build(); }

    public int size() { return records.size(); }

    void success(String id, String result) { if (notification != null) { notification.keyAction(id, result); } }

    void failure(String id, String result) { failures.put(id, result); }

    public static Builder bulkBuilder(String repo, String model, long timestamp, String user) {
        return new Builder(requireNonNull(repo), requireNonNull(model), timestamp, requireNonNull(user));
    }

    @RequiredArgsConstructor @Accessors(fluent = true, chain = true)
    public static class Builder {
        @NonNull private final String repo;
        @NonNull private final String model;
        private final long timestamp;
        @NonNull private final String user;
        @Setter private String parent;
        @Setter private boolean hasApproval;
        @Setter @NonNull private IgnoreErrors ignoreErrors = IgnoreErrors.STRICT;
        @Setter @NonNull private Validator validator = Validator.STRICT;
        @Setter private Notification.Builder notification;
        private final ImmutableListBuilder<IndexRecord> records = immutableListBuilder();

        public Builder withNotification() { return notification(notificationBuilder()); }

        public Builder withNotification(String realm) { return notification(notificationBuilder().realm(realm)); }

        public Builder hasApproval() { return hasApproval(true); }

        public Builder record(IndexRecord record) { this.records.add(checkParent(record)); return this; }

        public Builder records(Iterable<IndexRecord> records) { for (IndexRecord rec : records) { this.record(rec); } return this; }

        public Builder objects(Iterable<MapResult> docs) { for (MapResult doc : docs) { this.record(indexRec(doc).build()); } return this; }

        public Builder objectsWith(Iterable<MapResult> docs, String pipeline) {
            for (MapResult doc : docs) { this.record(indexRec(doc).pipeline(pipeline).build()); } return this;
        }

        private IndexRecord.Builder indexRec(MapResult doc) {
            IndexRecord.Builder builder = hasApproval ? createRecord(repo, approvalModel(model), id(doc), MetaFields.parent(doc), doc)
                .status(PENDING) : saveRecord(repo, model, id(doc), MetaFields.parent(doc), tstamp(doc), doc);
            return builder.userTs(user, timestamp);
        }

        private IndexRecord checkParent(IndexRecord record) {
            checkBool(Objects.equals(parent, record.parent), "bulk record cannot index child records for multiple parents"); return record;
        }

        public BulkRecord build() {
            List<IndexRecord> recordsList = records.build();
            if (notification != null) { recordsList.forEach(record -> notification.key(record.id, record.simpleKey())); }
            return new BulkRecord(repo, model, parent, recordsList, ignoreErrors, validator, notification);
        }
    }

    private static boolean isChangeAction(String action) { return "created".equals(action) || "updated".equals(action) || "deleted".equals(action); }
}