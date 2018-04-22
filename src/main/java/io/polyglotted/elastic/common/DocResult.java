package io.polyglotted.elastic.common;

import io.polyglotted.common.model.MapResult;
import lombok.RequiredArgsConstructor;
import org.elasticsearch.action.index.IndexRequest;

import static io.polyglotted.elastic.common.MetaFields.STATUS_FIELD;

@RequiredArgsConstructor
public final class DocResult {
    private final String index;
    public final String id;
    public final MapResult source;

    public String keyString() { return MetaFields.keyString(source); }

    public static MapResult docSource(DocResult result) { return result == null ? null : result.source; }

    public IndexRequest createRequest(MapResult ancillary, String parent, DocStatus status) {
        source.putAll(ancillary); source.put(STATUS_FIELD, status);
        return new IndexRequest(index, "_doc", id).create(false).routing(parent).source(source);
    }
}