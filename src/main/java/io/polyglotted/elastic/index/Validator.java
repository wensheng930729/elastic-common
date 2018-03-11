package io.polyglotted.elastic.index;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.EsAuth;
import io.polyglotted.elastic.common.MetaFields;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.index.IndexRequest;

import java.util.Map;

import static com.google.common.collect.Maps.filterKeys;
import static io.polyglotted.elastic.common.MetaFields.ANCESTOR_FIELD;
import static io.polyglotted.elastic.common.MetaFields.addMeta;
import static io.polyglotted.elastic.common.MetaFields.timestamp;
import static io.polyglotted.elastic.common.MetaFields.uniqueId;
import static io.polyglotted.elastic.index.RecordAction.CREATE;
import static io.polyglotted.elastic.search.Finder.findBy;

public interface Validator {
    Validator STRICT = new StrictValidator();
    Validator OVERRIDE = new OverwriteValidator();

    IndexRequest validate(ElasticClient client, EsAuth auth, IndexRecord record);

    @Slf4j class StrictValidator extends OverwriteValidator {
        @Override protected void validateCurrent(IndexRecord record, MapResult current) {
            String keyString = record.keyString();
            if (record.action != CREATE) {
                if (current == null) { throw new IndexerException(keyString + " - record not found for update"); }
                else if (record.baseVersion == null) { throw new IndexerException(keyString + " - baseVersion not found for update"); }
                else if (timestamp(current) != record.baseVersion) { throw new IndexerException(keyString + " - version conflict for update"); }
            }
            else if (current != null) { throw new IndexerException(keyString + " - record already exists"); }
        }
    }

    @Slf4j @SuppressWarnings({"unused", "WeakerAccess"}) class OverwriteValidator implements Validator {
        @Override public final IndexRequest validate(ElasticClient client, EsAuth auth, IndexRecord record) {
            preValidate(client, record);
            MapResult current = findBy(client, auth, record);
            if (isIdempotent(record, current)) { throw new NoopException(current); }
            validateCurrent(record, current);
            postValidate(record);
            return current == null ? null : createParentRequest(record, current);
        }

        protected void preValidate(ElasticClient client, IndexRecord record) { }

        protected void postValidate(IndexRecord record) { }

        protected void validateCurrent(IndexRecord record, MapResult current) { }

        public static IndexRequest createParentRequest(IndexRecord record, MapResult current) {
            String uniqueId = uniqueId(current);
            addMeta(record.source, ANCESTOR_FIELD, uniqueId);
            if (log.isTraceEnabled()) { log.trace("creating archive record " + uniqueId + " for " + record.simpleKey()); }
            return new IndexRequest(record.index, "_doc", uniqueId).create(true).source(record.update(current)).parent(record.parent);
        }
    }

    @SuppressWarnings("unchecked")
    static boolean isIdempotent(IndexRecord record, MapResult existing) {
        if (existing == null || record.action.notCreateOrUpdate() || !(record.source instanceof Map)) { return false; }
        Map<String, Object> current = filterKeys((MapResult) record.source, MetaFields::isNotMeta);
        return current.equals(filterKeys(existing, MetaFields::isNotMeta));
    }
}