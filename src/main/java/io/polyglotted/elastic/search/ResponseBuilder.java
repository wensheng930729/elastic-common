package io.polyglotted.elastic.search;

import io.polyglotted.elastic.common.Verbose;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;

import static io.polyglotted.common.util.CollUtil.transform;

public interface ResponseBuilder<T> {
    List<T> buildFrom(SearchResponse response, Verbose verbose);

    @SuppressWarnings("unused") ResponseBuilder<DeleteRequest> DeleteReqBuilder = (resp, v) -> transform(resp.getHits(),
        h -> new DeleteRequest(h.getIndex(), h.getType(), h.getId())).toList();
}