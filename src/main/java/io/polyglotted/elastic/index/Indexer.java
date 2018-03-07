package io.polyglotted.elastic.index;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.common.model.Pair;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.Notification;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.function.Consumer;

import static io.polyglotted.common.model.Pair.pair;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.elastic.index.IndexUtil.checkResponse;
import static io.polyglotted.elastic.index.IndexUtil.failOnIndex;
import static io.polyglotted.elastic.index.IndexUtil.keyOf;
import static io.polyglotted.elastic.index.IndexUtil.logError;
import static io.polyglotted.elastic.index.ServerException.serverException;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_OK;
import static org.elasticsearch.action.DocWriteRequest.OpType.CREATE;
import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.elasticsearch.search.fetch.subphase.FetchSourceContext.DO_NOT_FETCH_SOURCE;

@SuppressWarnings({"unused", "WeakerAccess"})
@RequiredArgsConstructor
public final class Indexer {
    private final ElasticClient client;

    public void lockTheIndexOrFail(String index, String keyString) { lockTheIndexOrFail(index, keyString, false); }

    public void lockTheIndexOrFail(String index, String keyString, boolean refresh) {
        IndexResponse response = client.index(new IndexRequest(index).id(keyString).opType(CREATE).source(ImmutableMap.of("i", 1)));
        if (response.status() != CREATED) { throw serverException("response failed while locking the keyString " + keyString); }
        if (refresh) { client.forceRefresh(index); }
    }

    public boolean checkLock(String index, String key) {
        return client.get(new GetRequest(index).id(key).fetchSourceContext(DO_NOT_FETCH_SOURCE)).isExists();
    }

    public void unlockIndex(String index, String key) { client.delete(new DeleteRequest(index).id(key)); client.forceRefresh(index); }

    public long generateSequence(String index, String unique) {
        return client.index(new IndexRequest(index).id(unique).source(ImmutableMap.of())).getVersion();
    }

    public void index(BulkRequest bulkRequest, IgnoreErrors ignoreErrors) {
        Pair<Boolean, String> errored = index(bulkRequest, ignoreErrors, null, null);
        failOnIndex(new IndexResult(errored._a ? SC_BAD_REQUEST : SC_OK, errored._b));
    }

    public Pair<Boolean, String> index(BulkRequest bulkRequest, IgnoreErrors ignoreErrors, Long timestamp, Notification.Builder notification) {
        if (bulkRequest.numberOfActions() <= 0) { return pair(false, "{}"); }
        BulkResponse responses = client.bulk(bulkRequest);

        return checkResponse(responses, ignoreErrors, nonNull(timestamp, -1L), notification);
    }

    public void indexAsync(BulkRequest bulkRequest, IgnoreErrors ignoreErrors, long timestamp, Consumer<IndexResult> consumer) {
        if (bulkRequest.numberOfActions() <= 0) { consumer.accept(new IndexResult(SC_OK, "")); return; }
        client.bulkAsync(bulkRequest, ActionListener.wrap(responses -> {
            Pair<Boolean, String> errored = checkResponse(responses, ignoreErrors, timestamp, null);
            consumer.accept(new IndexResult(errored._a ? SC_BAD_REQUEST : SC_OK, errored._b));
        }, IndexUtil::logUnknown));
    }

    @SneakyThrows
    public String bulkSave(IndexRecord record) {
        try {
            XContentBuilder result = XContentFactory.jsonBuilder().startObject();
            save(record.request(), record.timestamp(), result);
            client.forceRefresh(record.index);
            return result.endObject().string();

        } catch (RuntimeException ex) {
            throw logError(ex);
        }
    }

    public String strictSave(Pair<IndexRecord, IndexRecord> recordPair) { return strictSave(recordPair._a, recordPair._b, Validator.STRICT); }

    public String strictSave(IndexRecord record, Validator validator) { return strictSave(record, null, validator); }

    @SneakyThrows
    private String strictSave(IndexRecord primary, IndexRecord aux, Validator validator) {
        String lockString = nonNull(aux, primary).keyString;
        lockTheIndexOrFail(primary.index, lockString);
        try {
            XContentBuilder result = XContentFactory.jsonBuilder().startObject();
            writeStrict(primary, validator, aux == null ? result : null);
            if (aux != null) { writeStrict(aux, Validator.OVERRIDE, result); }
            return result.endObject().string();

        } catch (RuntimeException ex) {
            throw logError(ex);
        } finally { unlockIndex(primary.index, lockString); }
    }

    public BulkRequest validateRecord(IndexRecord record, BulkRequest bulkRequest, Validator validator) {
        IndexRequest archiveRequest = validator.validate(client, record);
        bulkRequest.add(record.request());
        if (archiveRequest != null) { bulkRequest.add(archiveRequest); }
        return bulkRequest;
    }

    private void writeStrict(IndexRecord record, Validator validator, XContentBuilder result) {
        IndexRequest archiveRequest = validator.validate(client, record);
        save(record.request(), record.timestamp(), result);
        if (archiveRequest != null) { client.index(archiveRequest); }
    }

    public void save(DocWriteRequest<?> request, String index) { save(request, 0L, null); client.forceRefresh(index); }

    private void save(DocWriteRequest<?> request, long timestamp, XContentBuilder result) {
        DocWriteResponse response = (request instanceof IndexRequest) ? client.index((IndexRequest) request) :
            ((request instanceof DeleteRequest) ? client.delete((DeleteRequest) request) : null);
        if (response != null && result != null) { keyOf(response, timestamp, result); }
    }
}