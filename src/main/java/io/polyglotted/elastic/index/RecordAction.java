package io.polyglotted.elastic.index;

import io.polyglotted.common.model.HasMeta;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.common.DocStatus;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Map;

import static io.polyglotted.common.util.BaseSerializer.serializeMeta;

@Slf4j @SuppressWarnings("unused")
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum RecordAction {
    CREATE(DocStatus.LIVE, "creating", false),
    UPDATE(DocStatus.UPDATED, "updating", false),
    DELETE(DocStatus.DELETED, "deleting", true),
    APPROVE(DocStatus.LIVE, "deleting", true),
    REJECT(DocStatus.UPDATED, "updating", false),
    DISCARD(DocStatus.DISCARDED, "deleting", true);

    public final DocStatus status;
    public final String message;
    public final boolean isDelete;

    public boolean notCreateOrUpdate() { return this != CREATE && this != UPDATE; }

    public DocWriteRequest<?> request(IndexRecord record) {
        if (log.isTraceEnabled()) { log.trace(message + " record " + record.id + " for " + record.model + " at " + record.index); }
        return isDelete ? new DeleteRequest(record.index).id(record.id).parent(record.parent)
            : detectSource(new IndexRequest(record.index).id(record.id), record.pipeline, record.source).parent(record.parent);
    }

    @SuppressWarnings("unchecked")
    private static IndexRequest detectSource(IndexRequest request, String pipeline, Object source) {
        if (source instanceof MapResult) { request.source((Map) source, (pipeline == null ? XContentType.JSON : XContentType.CBOR)); }
        else if (source instanceof HasMeta) { request.source(serializeMeta((HasMeta) source), XContentType.JSON); }
        else { throw new IllegalArgumentException("unknown source for indexing"); }
        return request.setPipeline(pipeline);
    }
}