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
import static io.polyglotted.common.util.CollUtil.transform;
import static io.polyglotted.common.util.ListBuilder.immutableListBuilder;
import static io.polyglotted.common.util.MapBuilder.simpleMap;
import static io.polyglotted.elastic.common.MetaFields.id;
import static io.polyglotted.elastic.common.MetaFields.tstamp;
import static io.polyglotted.elastic.common.Notification.notificationBuilder;
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

    void success(String id, String result) { if (notification != null) { notification.keyAction(id, result); } }

    void failure(String id, String result) { failures.put(id, result); }

    public static Builder bulkBuilder(String repo, String model, long timestamp, String user) {
        return new Builder(repo, model, timestamp, requireNonNull(user));
    }

    @RequiredArgsConstructor @Accessors(fluent = true, chain = true)
    public static class Builder {
        private final String repo;
        private final String model;
        private final long timestamp;
        private final String user;
        @Setter private String parent;
        @Setter @NonNull private IgnoreErrors ignoreErrors = IgnoreErrors.STRICT;
        @Setter @NonNull private Validator validator = Validator.STRICT;
        @Setter private Notification.Builder notification;
        private final ImmutableListBuilder<IndexRecord> records = immutableListBuilder();

        public Builder withNotification() { return notification(notificationBuilder()); }

        public Builder record(IndexRecord record) { this.records.add(checkParent(record)); return this; }

        public Builder records(Iterable<IndexRecord> records) { for (IndexRecord rec : records) { this.record(rec); } return this; }

        public Builder objects(Iterable<MapResult> objects) { return records(transform(objects, this::indexRec)); }

        private IndexRecord indexRec(MapResult object) {
            return checkParent(saveRecord(requireNonNull(repo), requireNonNull(model), id(object), MetaFields.parent(object),
                tstamp(object), object).timestamp(timestamp).user(user).build());
        }

        private IndexRecord checkParent(IndexRecord record) {
            checkBool(Objects.equals(parent, record.parent), "bulk record cannot index child records for multiple parents"); return record;
        }

        public BulkRecord build() {
            List<IndexRecord> recordsList = records.build();
            if (notification != null) { recordsList.forEach(record -> notification.key(record.id, record.simpleKey())); }
            return new BulkRecord(model, repo, parent, recordsList, ignoreErrors, validator, notification);
        }
    }

    private static boolean isChangeAction(String action) { return "created".equals(action) || "updated".equals(action) || "deleted".equals(action); }
}