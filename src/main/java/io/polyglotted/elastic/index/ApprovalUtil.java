package io.polyglotted.elastic.index;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.common.model.Pair;
import io.polyglotted.elastic.common.DocResult;
import io.polyglotted.elastic.common.DocStatus;
import io.polyglotted.elastic.search.Expressions.BoolBuilder;
import io.polyglotted.elastic.search.Searcher;
import org.elasticsearch.action.search.SearchRequest;

import java.util.List;

import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.elastic.common.DocStatus.DISCARDED;
import static io.polyglotted.elastic.common.DocStatus.PENDING;
import static io.polyglotted.elastic.common.DocStatus.PENDING_DELETE;
import static io.polyglotted.elastic.common.DocStatus.REJECTED;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.MODEL_FIELD;
import static io.polyglotted.elastic.common.Verbose.NONE;
import static io.polyglotted.elastic.index.RecordAction.APPROVE;
import static io.polyglotted.elastic.index.RecordAction.CREATE;
import static io.polyglotted.elastic.index.RecordAction.DELETE;
import static io.polyglotted.elastic.index.ValidateException.validateNotNull;
import static io.polyglotted.elastic.search.Expressions.bool;
import static io.polyglotted.elastic.search.Expressions.equalsTo;
import static io.polyglotted.elastic.search.Finder.idBuilder;
import static io.polyglotted.elastic.search.QueryMaker.filterToRequest;
import static io.polyglotted.elastic.search.ResponseBuilder.DocResultBuilder;
import static org.elasticsearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE;

@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class ApprovalUtil {
    public static String approvalModel(String model) { return model + "$approval"; }

    public static BoolBuilder pendingBuilder(String m, String id) { return idBuilder(approvalModel(m), equalsTo(ID_FIELD, id)).pendingApproval(); }

    public static DocResult fetchApprovalDoc(Searcher searcher, AuthHeader esAuth, String repo, String model, String id) {
        return validateNotNull(fetchPendingDoc(searcher, esAuth, repo, model, id, bool().pendingApproval()),
            "cannot find approval document " + id + " for model " + model);
    }

    public static DocResult fetchPendingDoc(Searcher searcher, AuthHeader esAuth, String repo, String model, String id, BoolBuilder boolBuilder) {
        SearchRequest searchRequest = filterToRequest(repo, boolBuilder.musts(equalsTo(MODEL_FIELD, approvalModel(model)),
            equalsTo(ID_FIELD, id)).build(), FETCH_SOURCE, immutableList(), 1);
        List<DocResult> response = searcher.searchBy(esAuth, searchRequest, DocResultBuilder, NONE).resultsAs(DocResult.class);
        return response.size() > 0 ? response.get(0) : null;
    }

    public static Pair<IndexRecord, IndexRecord> approvePair(DocResult approvalDoc, String comment, long millis, String user) {
        DocStatus newStatus = checkStatus(approvalDoc);
        IndexRecord approvalRec = approvalDoc.recordOf(newStatus == PENDING ? APPROVE : DELETE)
            .userTs(user, millis).comment(comment, false).build();

        RecordAction newAction = (newStatus == PENDING_DELETE) ? DELETE : CREATE;
        return Pair.pair(approvalRec, approvalDoc.recordOf(newAction, approvalDoc.nakedModel(), true).userTs(user, millis).build());
    }

    public static IndexRecord reject(DocResult approvalDoc, RecordAction action, String comment, long millis, String user) {
        checkStatus(approvalDoc); return approvalDoc.recordOf(action).status(REJECTED).userTs(user, millis).comment(comment, true).build();
    }

    public static IndexRecord discard(DocResult approvalDoc, RecordAction action, String comment, long millis, String user) {
        checkStatus(approvalDoc); return approvalDoc.recordOf(action).status(DISCARDED).userTs(user, millis).comment(comment, true).build();
    }

    private static DocStatus checkStatus(DocResult existing) {
        DocStatus status = existing.status();
        if (status != PENDING && status != PENDING_DELETE) { throw new ValidateException(403, "status invalid for approval " + status); }
        return status;
    }
}