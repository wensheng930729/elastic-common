package io.polyglotted.elastic.index;

import io.polyglotted.common.model.HasMeta;
import io.polyglotted.common.model.Jsoner;
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
    CREATE(DocStatus.UPDATED, "creating", false),
    UPDATE(DocStatus.UPDATED, "updating", false),
    DELETE(DocStatus.DELETED, "deleting", true),
    APPROVE(DocStatus.LIVE, "deleting", true),
    REJECT(DocStatus.UPDATED, "updating", false),
    DISCARD(DocStatus.DISCARDED, "deleting", true);

    public final DocStatus status;
    public final String message;
    public final boolean isDelete;

    public boolean notCreateOrUpdate() { return this != CREATE && this != UPDATE; }

    public String approvalComment() { return this == REJECT ? "rejected by user" : (this == DISCARD ? "discarded by user" : "approved by user"); }

    public DocWriteRequest<?> request(IndexRecord record) {
        if (log.isTraceEnabled()) { log.trace(message + " record for " + record.model + " at " + record.repo); }
        return isDelete ? new DeleteRequest(record.repo, "_doc", record.ancestorId()).routing(record.parent)
            : detectSource(new IndexRequest(record.repo, "_doc"), record.pipeline, record.source).routing(record.parent);
    }

    @SuppressWarnings("unchecked") private static IndexRequest detectSource(IndexRequest request, String pipeline, Object source) {
        if (source instanceof Jsoner) { request.source(((Jsoner) source).toJson(), XContentType.JSON); }
        else if (source instanceof HasMeta) { request.source(serializeMeta((HasMeta) source), XContentType.JSON); }
        else if (source instanceof Map) { request.source((Map) source, (pipeline == null ? XContentType.JSON : XContentType.CBOR)); }
        else { throw new IllegalArgumentException("unknown source for indexing"); }
        return request.setPipeline(pipeline);
    }
}