package io.polyglotted.elastic.search;

import io.polyglotted.elastic.common.DocResult;
import io.polyglotted.elastic.common.Verbose;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;

import static io.polyglotted.common.util.CollUtil.transform;
import static io.polyglotted.elastic.search.SearchUtil.hitSource;

@SuppressWarnings("unused")
public interface ResponseBuilder<T> {
    List<T> buildFrom(SearchResponse response, Verbose verbose);

    ResponseBuilder<DeleteRequest> DeleteReqBuilder = (resp, v) -> transform(resp.getHits(),
        h -> new DeleteRequest(h.getIndex(), h.getType(), h.getId())).toList();
    ResponseBuilder<DocResult> DocResultBuilder = (resp, v) -> transform(resp.getHits(),
        h -> new DocResult(h.getIndex(), h.getId(), hitSource(h))).toList();
}