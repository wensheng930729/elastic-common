package io.polyglotted.elastic.index;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.DocResult;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;

import java.util.Map;

import static io.polyglotted.elastic.common.DocResult.filtered;
import static io.polyglotted.elastic.common.MetaFields.reqdKey;
import static io.polyglotted.elastic.common.MetaFields.timestamp;
import static io.polyglotted.elastic.index.RecordAction.CREATE;
import static io.polyglotted.elastic.search.Finder.findAll;
import static io.polyglotted.elastic.search.Finder.findById;
import static org.elasticsearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE;

public interface Validator {
    Validator STRICT = new StrictValidator();
    Validator OVERRIDE = new OverwriteValidator();

    BulkRequest validateAll(ElasticClient client, AuthHeader auth, BulkRecord bulkRecord, BulkRequest bulkRequest);

    IndexRequest validate(ElasticClient client, AuthHeader auth, IndexRecord record);

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
        @Override public BulkRequest validateAll(ElasticClient client, AuthHeader auth, BulkRecord bulkRecord, BulkRequest bulkRequest) {
            Map<String, DocResult> docs = findAll(client, auth, bulkRecord);
            for (IndexRecord record : bulkRecord.records) {
                try {
                    preValidate(client, record);
                    IndexRequest ancestor = checkRecordWithDoc(record, docs.get(record.keyString()));
                    bulkRequest.add(record.request());
                    if (ancestor != null) { bulkRequest.add(ancestor); }
                } catch (NoopException nex) {
                    bulkRecord.successWith(record.id, nex.getMessage());
                } catch (Exception ex) {
                    bulkRecord.failureWith(record.id, ex.getMessage());
                }
            }
            return bulkRequest;
        }

        @Override public final IndexRequest validate(ElasticClient client, AuthHeader auth, IndexRecord record) {
            preValidate(client, record);
            DocResult existing = findById(client, auth, record.repo, record.model, record.id, record.parent, FETCH_SOURCE);
            return checkRecordWithDoc(record, existing);
        }

        protected IndexRequest checkRecordWithDoc(IndexRecord record, DocResult existing) {
            if (isIdempotent(record, existing)) { throw new NoopException(existing.source); }
            validateCurrent(record, existing == null ? null : existing.source);
            postValidate(record);
            return existing == null ? null : createParentRequest(record, existing);
        }

        protected void preValidate(ElasticClient client, IndexRecord record) { }

        protected void postValidate(IndexRecord record) { }

        protected void validateCurrent(IndexRecord record, MapResult current) { }

        public static IndexRequest createParentRequest(IndexRecord record, DocResult ancestor) {
            record.update(ancestor.id, reqdKey(ancestor.source), record.action.status);
            if (log.isTraceEnabled()) { log.trace("creating archive record for " + record.simpleKey()); }
            return ancestor.ancestorRequest(record.repo, record.ancillary, record.parent, record.action.status);
        }
    }

    @SuppressWarnings("unchecked")
    static boolean isIdempotent(IndexRecord record, DocResult existing) {
        if (existing == null || record.action.notCreateOrUpdate() || !(record.source instanceof Map)) { return false; }
        Map<String, Object> current = filtered((MapResult) record.source);
        return current.equals(filtered(existing.source));
    }
}