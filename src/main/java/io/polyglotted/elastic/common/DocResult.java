package io.polyglotted.elastic.common;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.index.IndexRecord;
import io.polyglotted.elastic.index.RecordAction;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.index.IndexRequest;

import java.util.Map;

import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.util.CollUtil.filterKeys;
import static io.polyglotted.common.util.StrUtil.safePrefix;
import static io.polyglotted.elastic.common.MetaFields.STATUS_FIELD;
import static io.polyglotted.elastic.common.MetaFields.model;
import static io.polyglotted.elastic.common.MetaFields.parent;
import static io.polyglotted.elastic.common.MetaFields.reqdId;
import static io.polyglotted.elastic.common.MetaFields.timestamp;
import static io.polyglotted.elastic.index.IndexRecord.expired;
import static java.util.Objects.requireNonNull;

@RequiredArgsConstructor
public final class DocResult {
    @NonNull private final String index;
    @NonNull public final String id;
    @NonNull public final MapResult source;

    public boolean hasMeta(String meta) { return source.containsKey(meta); }

    public String keyString() { return MetaFields.keyString(source); }

    public String nakedModel() { return safePrefix(model(source), "$"); }

    public DocStatus status() { return MetaFields.status(source); }

    public static MapResult docSource(DocResult result) { return result == null ? null : result.source; }

    public IndexRequest ancestorRequest(MapResult ancillary, String parent, DocStatus status) {
        source.putAll(ancillary); source.put(STATUS_FIELD, status.name());
        return new IndexRequest(index, "_doc", id).create(false).routing(parent).source(source);
    }

    public IndexRecord.Builder recordOf(RecordAction action) { return recordOf(action, model(source), false); }

    public IndexRecord.Builder recordOf(RecordAction action, String model, boolean filter) {
        return expired(action, index, requireNonNull(model), reqdId(source), parent(source),
            timestamp(source), filter ? simpleResult(filtered(source)) : source);
    }

    public static Map<String, Object> filtered(MapResult source) { return filterKeys(source, MetaFields::isNotMeta); }
}