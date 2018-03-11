package io.polyglotted.elastic.index;

import com.google.common.collect.ImmutableList;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.ListBuilder.ImmutableListBuilder;
import io.polyglotted.elastic.common.EsAuth;
import io.polyglotted.elastic.common.Notification;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.elasticsearch.action.bulk.BulkRequest;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.transform;
import static io.polyglotted.common.util.ListBuilder.immutableListBuilder;
import static io.polyglotted.common.util.MapBuilder.simpleMap;
import static io.polyglotted.elastic.common.MetaFields.id;
import static io.polyglotted.elastic.common.Notification.notificationBuilder;
import static io.polyglotted.elastic.index.IndexRecord.createRecord;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;

@SuppressWarnings({"unused", "WeakerAccess", "StaticPseudoFunctionalStyleMethod"})
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class BulkRecord {
    public final String model;
    public final long timestamp;
    public final List<IndexRecord> records;
    public final IgnoreErrors ignoreErrors;
    private final Validator validator;
    private final Notification.Builder notification;
    public final Map<String, String> failures = simpleMap();

    BulkRequest bulkRequest(EsAuth esAuth, Indexer indexer) {
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(IMMEDIATE);
        records.forEach(record -> {
            try {
                indexer.validateRecord(esAuth, record, bulkRequest, validator);
            } catch (NoopException noop) { success(record.id, "noop"); }
        });
        return bulkRequest;
    }

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
        @Setter @NonNull private IgnoreErrors ignoreErrors = IgnoreErrors.STRICT;
        @Setter @NonNull private Validator validator = Validator.STRICT;
        @Setter private Notification.Builder notification;
        private final ImmutableListBuilder<IndexRecord> records = immutableListBuilder();

        public Builder withNotification() { return notification(notificationBuilder()); }

        public Builder record(IndexRecord record) { this.records.add(record); return this; }

        public Builder records(Iterable<IndexRecord> records) { this.records.addAll(records); return this; }

        public Builder objects(Iterable<MapResult> objects) { return records(transform(objects, this::indexRec)); }

        private IndexRecord indexRec(MapResult object) {
            return createRecord(requireNonNull(repo), requireNonNull(model), id(object), object).timestamp(timestamp).user(user).build();
        }

        public BulkRecord build() {
            ImmutableList<IndexRecord> recordsList = records.build();
            if (notification != null) { recordsList.forEach(record -> notification.key(record.id, record.key())); }
            return new BulkRecord(model, timestamp, recordsList, ignoreErrors, validator, notification);
        }
    }

    private static boolean isChangeAction(String action) { return "created".equals(action) || "updated".equals(action) || "deleted".equals(action); }
}