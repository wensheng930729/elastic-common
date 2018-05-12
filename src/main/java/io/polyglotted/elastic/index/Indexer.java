package io.polyglotted.elastic.index;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.common.model.Pair;
import io.polyglotted.elastic.client.ElasticClient;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.function.BiConsumer;

import static io.polyglotted.common.util.MapBuilder.immutableMap;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.MODEL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.RESULT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TIMESTAMP_FIELD;
import static io.polyglotted.elastic.index.Validator.STRICT;
import static org.elasticsearch.action.DocWriteRequest.OpType.CREATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.rest.RestStatus.CREATED;

@Slf4j @SuppressWarnings({"unused", "WeakerAccess"})
@RequiredArgsConstructor
public final class Indexer {
    private final ElasticClient client;

    public void lockTheIndexOrFail(AuthHeader auth, String index, String keyString) { lockTheIndexOrFail(auth, index, keyString, false); }

    public void lockTheIndexOrFail(AuthHeader auth, String index, String keyString, boolean refresh) {
        IndexResponse response = client.index(auth, new IndexRequest(index, "_doc", keyString)
            .opType(CREATE).source(immutableMap(TIMESTAMP_FIELD, 1)));
        if (response.status() != CREATED) { throw new IndexerException("response failed while locking the keyString " + keyString); }
        if (refresh) { client.forceRefresh(auth, index); }
    }

    public boolean checkLock(AuthHeader auth, String index, String key) { return client.exists(auth, new GetRequest(index, "_doc", key)); }

    public void unlockIndex(AuthHeader auth, String index, String key) {
        client.delete(auth, new DeleteRequest(index, "_doc", key)); client.forceRefresh(auth, index);
    }

    public long generateSequence(AuthHeader auth, String index, String key) {
        return client.index(auth, new IndexRequest(index, "_doc", key).source(immutableMap())).getVersion();
    }

    public boolean bulkSave(AuthHeader auth, BulkRecord bulkRecord) {
        try {
            BulkRequest bulkRequest = bulkRecord.validator.validateAll(client, auth, bulkRecord, new BulkRequest().setRefreshPolicy(IMMEDIATE));
            if (bulkRequest.numberOfActions() <= 0) { return true; }
            BulkResponse responses = client.bulk(auth, bulkRequest);
            return checkResponse(responses, bulkRecord.ignoreErrors, bulkRecord::success, bulkRecord::failure);
        } catch (RuntimeException ex) { throw logError(ex); }
    }

    public boolean strictSave(AuthHeader auth, BulkRecord bulkRecord) {
        lockTheIndexOrFail(auth, bulkRecord.repo, bulkRecord.model);
        try {
            return bulkSave(auth, bulkRecord);
        } finally { unlockIndex(auth, bulkRecord.repo, bulkRecord.model); }
    }

    @SneakyThrows public String bulkSave(AuthHeader auth, IndexRecord record) {
        try {
            XContentBuilder result = XContentFactory.jsonBuilder().startObject();
            save(auth, record, result);
            client.forceRefresh(auth, record.index);
            return result.endObject().string();

        } catch (NoopException nex) {
            return nex.getMessage();
        } catch (RuntimeException ex) { throw logError(ex); }
    }

    public void validateRecord(AuthHeader auth, IndexRecord record, BulkRequest bulkRequest, Validator validator) {
        IndexRequest archiveRequest = validator.validate(client, auth, record);
        bulkRequest.add(record.request());
        if (archiveRequest != null) { bulkRequest.add(archiveRequest); }
    }

    public String strictSave(AuthHeader auth, Pair<IndexRecord, IndexRecord> pair) { return strictSave(auth, pair._a, pair._b, STRICT); }

    public String strictSave(AuthHeader auth, IndexRecord record, Validator validator) { return strictSave(auth, record, null, validator); }

    @SneakyThrows
    private String strictSave(AuthHeader auth, IndexRecord primary, IndexRecord aux, Validator validator) {
        if (checkLock(auth, primary.index, primary.model)) { throw new IndexerException("index-model locked for write"); }

        String lockString = nonNull(aux, primary).lockString();
        lockTheIndexOrFail(auth, primary.index, lockString);
        try {
            XContentBuilder result = XContentFactory.jsonBuilder().startObject();
            writeStrict(auth, primary, validator, aux == null ? result : null);
            if (aux != null) { writeStrict(auth, aux, Validator.OVERRIDE, result); }
            return result.endObject().string();

        } catch (NoopException nex) {
            return nex.getMessage();
        } catch (RuntimeException ex) {
            throw logError(ex);
        } finally { unlockIndex(auth, primary.index, lockString); }
    }

    private void writeStrict(AuthHeader auth, IndexRecord record, Validator validator, XContentBuilder result) {
        IndexRequest archiveRequest = validator.validate(client, auth, record);
        save(auth, record, result);
        if (archiveRequest != null) { client.index(auth, archiveRequest); }
    }

    @SneakyThrows private void save(AuthHeader auth, IndexRecord record, XContentBuilder result) {
        DocWriteRequest<?> request = record.request();
        DocWriteResponse response = (request instanceof IndexRequest) ? client.index(auth, (IndexRequest) request) :
            ((request instanceof DeleteRequest) ? client.delete(auth, (DeleteRequest) request) : null);
        if (response != null && result != null) {
            result.field(MODEL_FIELD, record.model);
            result.field(ID_FIELD, record.id);
            result.field(TIMESTAMP_FIELD, record.timestamp);
            result.field(RESULT_FIELD, nonNull(record.getResult(), response.getResult().getLowercase()));
        }
    }

    private static boolean checkResponse(BulkResponse responses, IgnoreErrors ignore, BiConsumer<String, String> successHandler,
                                         BiConsumer<String, String> failureHandler) {
        boolean noErrors = true;
        for (BulkItemResponse response : responses) {
            if (response.isFailed()) {
                String failureMessage = response.getFailureMessage();
                if (!ignore.ignoreFailure(failureMessage)) {
                    noErrors = false; failureHandler.accept(response.getId(), failureMessage);
                }
            }
            else { successHandler.accept(response.getId(), response.getResponse().getResult().getLowercase()); }
        }
        return noErrors;
    }

    private static RuntimeException logError(RuntimeException ex) {
        if (ex instanceof IndexerException) { log.error("two phase commit failed: " + ex.getMessage()); return ex; }
        else { log.error("two phase commit failed: " + ex.getMessage(), ex); return new IndexerException(ex.getMessage(), ex); }
    }
}