package io.polyglotted.elastic.index;

import com.google.common.collect.ImmutableMap;
import io.polyglotted.common.model.Pair;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.EsAuth;
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
import static io.polyglotted.elastic.index.Validator.STRICT;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_OK;
import static org.elasticsearch.action.DocWriteRequest.OpType.CREATE;
import static org.elasticsearch.rest.RestStatus.CREATED;
import static org.elasticsearch.search.fetch.subphase.FetchSourceContext.DO_NOT_FETCH_SOURCE;

@SuppressWarnings({"unused", "WeakerAccess"})
@RequiredArgsConstructor
public final class Indexer {
    private final ElasticClient clienta;

    public void lockTheIndexOrFail(EsAuth auth, String index, String keyString) { lockTheIndexOrFail(auth, index, keyString, false); }

    public void lockTheIndexOrFail(EsAuth auth, String index, String keyString, boolean refresh) {
        IndexResponse response = clienta.index(auth, new IndexRequest(index).id(keyString).opType(CREATE).source(ImmutableMap.of("i", 1)));
        if (response.status() != CREATED) { throw serverException("response failed while locking the keyString " + keyString); }
        if (refresh) { clienta.forceRefresh(auth, index); }
    }

    public boolean checkLock(EsAuth auth, String index, String key) {
        return clienta.get(auth, new GetRequest(index).id(key).fetchSourceContext(DO_NOT_FETCH_SOURCE)).isExists();
    }

    public void unlockIndex(EsAuth auth, String index, String key) {
        clienta.delete(auth, new DeleteRequest(index).id(key)); clienta.forceRefresh(auth, index);
    }

    public long generateSequence(EsAuth auth, String index, String unique) {
        return clienta.index(auth, new IndexRequest(index).id(unique).source(ImmutableMap.of())).getVersion();
    }

    public void index(EsAuth auth, BulkRequest bulkRequest, IgnoreErrors ignoreErrors) {
        Pair<Boolean, String> errored = index(auth, bulkRequest, ignoreErrors, null, null);
        failOnIndex(new IndexResult(errored._a ? SC_BAD_REQUEST : SC_OK, errored._b));
    }

    public Pair<Boolean, String> index(EsAuth auth, BulkRequest bulkRequest, IgnoreErrors ignoreErrors,
                                       Long timestamp, Notification.Builder notification) {
        if (bulkRequest.numberOfActions() <= 0) { return pair(false, "{}"); }
        BulkResponse responses = clienta.bulk(auth, bulkRequest);

        return checkResponse(responses, ignoreErrors, nonNull(timestamp, -1L), notification);
    }

    public void indexAsync(EsAuth auth, BulkRequest bulkRequest, IgnoreErrors ignoreErrors, long timestamp, Consumer<IndexResult> consumer) {
        if (bulkRequest.numberOfActions() <= 0) { consumer.accept(new IndexResult(SC_OK, "")); return; }
        clienta.bulkAsync(auth, bulkRequest, ActionListener.wrap(responses -> {
            Pair<Boolean, String> errored = checkResponse(responses, ignoreErrors, timestamp, null);
            consumer.accept(new IndexResult(errored._a ? SC_BAD_REQUEST : SC_OK, errored._b));
        }, IndexUtil::logUnknown));
    }

    @SneakyThrows
    public String bulkSave(EsAuth auth, IndexRecord record) {
        try {
            XContentBuilder result = XContentFactory.jsonBuilder().startObject();
            save(auth, record.request(), record.timestamp(), result);
            clienta.forceRefresh(auth, record.index);
            return result.endObject().string();

        } catch (RuntimeException ex) { throw logError(ex); }
    }

    public String strictSave(EsAuth auth, Pair<IndexRecord, IndexRecord> pair) { return strictSave(auth, pair._a, pair._b, STRICT); }

    public String strictSave(EsAuth auth, IndexRecord record, Validator validator) { return strictSave(auth, record, null, validator); }

    @SneakyThrows
    private String strictSave(EsAuth auth, IndexRecord primary, IndexRecord aux, Validator validator) {
        String lockString = nonNull(aux, primary).keyString;
        lockTheIndexOrFail(auth, primary.index, lockString);
        try {
            XContentBuilder result = XContentFactory.jsonBuilder().startObject();
            writeStrict(auth, primary, validator, aux == null ? result : null);
            if (aux != null) { writeStrict(auth, aux, Validator.OVERRIDE, result); }
            return result.endObject().string();

        } catch (RuntimeException ex) {
            throw logError(ex);
        } finally { unlockIndex(auth, primary.index, lockString); }
    }

    public BulkRequest validateRecord(EsAuth auth, IndexRecord record, BulkRequest bulkRequest, Validator validator) {
        IndexRequest archiveRequest = validator.validate(clienta, auth, record);
        bulkRequest.add(record.request());
        if (archiveRequest != null) { bulkRequest.add(archiveRequest); }
        return bulkRequest;
    }

    private void writeStrict(EsAuth auth, IndexRecord record, Validator validator, XContentBuilder result) {
        IndexRequest archiveRequest = validator.validate(clienta, auth, record);
        save(auth, record.request(), record.timestamp(), result);
        if (archiveRequest != null) { clienta.index(auth, archiveRequest); }
    }

    public void save(EsAuth auth, DocWriteRequest<?> request, String index) {
        save(auth, request, 0L, null); clienta.forceRefresh(auth, index);
    }

    private void save(EsAuth auth, DocWriteRequest<?> request, long timestamp, XContentBuilder result) {
        DocWriteResponse response = (request instanceof IndexRequest) ? clienta.index(auth, (IndexRequest) request) :
            ((request instanceof DeleteRequest) ? clienta.delete(auth, (DeleteRequest) request) : null);
        if (response != null && result != null) { keyOf(response, timestamp, result); }
    }
}