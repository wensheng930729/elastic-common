package io.polyglotted.elastic.test;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.index.BulkRecord;
import io.polyglotted.elastic.index.Indexer;
import io.polyglotted.elastic.index.IndexerException;
import io.polyglotted.elastic.search.Expression;
import io.polyglotted.elastic.search.Searcher;
import org.elasticsearch.action.search.SearchRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.util.BaseSerializer.serialize;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.common.util.ResourceUtil.readResourceAsMap;
import static io.polyglotted.elastic.admin.IndexRequestor.indexFile;
import static io.polyglotted.elastic.admin.IndexSetting.with;
import static io.polyglotted.elastic.admin.Type.typeBuilder;
import static io.polyglotted.elastic.common.DocStatus.PENDING;
import static io.polyglotted.elastic.common.DocStatus.PENDING_DELETE;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TIMESTAMP_FIELD;
import static io.polyglotted.elastic.common.Verbose.META;
import static io.polyglotted.elastic.index.ApprovalUtil.approvalModel;
import static io.polyglotted.elastic.index.ApprovalUtil.approvePair;
import static io.polyglotted.elastic.index.ApprovalUtil.discard;
import static io.polyglotted.elastic.index.ApprovalUtil.fetchApprovalDoc;
import static io.polyglotted.elastic.index.ApprovalUtil.fetchPendingDoc;
import static io.polyglotted.elastic.index.ApprovalUtil.reject;
import static io.polyglotted.elastic.index.IndexRecord.createRecord;
import static io.polyglotted.elastic.index.IndexRecord.updateRecord;
import static io.polyglotted.elastic.index.RecordAction.DISCARD;
import static io.polyglotted.elastic.index.RecordAction.REJECT;
import static io.polyglotted.elastic.index.Validator.STRICT;
import static io.polyglotted.elastic.search.Expressions.bool;
import static io.polyglotted.elastic.search.Expressions.equalsTo;
import static io.polyglotted.elastic.search.QueryMaker.filterToRequest;
import static io.polyglotted.elastic.search.ResultBuilder.SourceBuilder;
import static io.polyglotted.elastic.test.ElasticTestUtil.ES_AUTH;
import static io.polyglotted.elastic.test.ElasticTestUtil.testElasticClient;
import static org.elasticsearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;
import static org.elasticsearch.search.sort.SortOrder.DESC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ApprovalIntegTest {
    private static final Map<String, String> MESSAGES = readResourceAsMap(ApprovalIntegTest.class, "approval-integ.txt");
    private static final String Tester = "tester", Approver = "approver";
    private static final long T1 = 1000, T2 = 2000, T3 = 3000, T4 = 4000, T5 = 5000, T6 = 6000, T7 = 7000;
    private static final String REPO = "AppSvc", MODEL = "Entity";
    private ElasticClient client;
    private Indexer indexer;
    private Searcher searcher;

    @Before public void init() {
        client = testElasticClient(); indexer = new Indexer(client); searcher = new Searcher(client);
    }

    @After public void close() { client.close(); }

    @Test
    public void approvalLifeCycle() {
        String index2 = client.createIndex(indexFile(with(3, 0), typeBuilder().build(), REPO));
        try {
            indexer.strictSave(ES_AUTH, createRecord(REPO, approvalModel(MODEL), "neon", simpleResult("name", "Neon"))
                .status(PENDING).userTs(Tester, T1).build(), STRICT);
            checkStatusObjects(searcher, bool().liveIndex().build(), 0);
            checkStatusObjects(searcher, bool().pendingApproval().build(), 1);
            indexer.strictSave(ES_AUTH, approvePair(REPO, fetchApprovalDoc(searcher, ES_AUTH, REPO, MODEL, "neon"), null, T2, Approver));
            checkStatusObjects(searcher, bool().liveIndex().build(), 1);
            checkStatusObjects(searcher, bool().pendingApproval().build(), 0);

            indexer.strictSave(ES_AUTH, createRecord(REPO, approvalModel(MODEL), "neon", simpleResult("name", "Neon2"))
                .status(PENDING).userTs(Tester, T3).build(), STRICT);
            checkStatusObjects(searcher, bool().liveIndex().must(equalsTo(TIMESTAMP_FIELD, T3)).build(), 0);
            checkStatusObjects(searcher, bool().pendingApproval().build(), 1);
            indexer.strictSave(ES_AUTH, updateRecord(REPO, approvalModel(MODEL), "neon", T3, simpleResult("name", "Neonite"))
                .status(PENDING).userTs(Tester, T4).build(), STRICT);
            checkStatusObjects(searcher, bool().pendingApproval().build(), 1);
            indexer.strictSave(ES_AUTH, approvePair(REPO, fetchApprovalDoc(searcher, ES_AUTH, REPO, MODEL, "neon"), null, T5, Approver));
            checkStatusObjects(searcher, bool().liveIndex().build(), 1);
            checkStatusObjects(searcher, bool().pendingApproval().build(), 0);

            indexer.strictSave(ES_AUTH, createRecord(REPO, approvalModel(MODEL), "neon", simpleResult("name", "Neon2"))
                .status(PENDING_DELETE).userTs(Tester, T6).build(), STRICT);
            checkStatusObjects(searcher, bool().pendingApproval().build(), 1);
            indexer.strictSave(ES_AUTH, approvePair(REPO, fetchApprovalDoc(searcher, ES_AUTH, REPO, MODEL, "neon"), null, T7, Approver));
            checkStatusObjects(searcher, bool().liveIndex().build(), 0);
            checkStatusObjects(searcher, bool().pendingApproval().build(), 0);

            String actual = checkStatusObjects(searcher, bool().allIndex().build(), 6);
            assertThat(actual, actual, is(MESSAGES.get("approval.lc")));

        } finally { client.dropIndex(index2); }
    }

    @Test
    public void approvalRejectDiscard() {
        String index2 = client.createIndex(indexFile(with(3, 0), typeBuilder().build(), REPO));
        try {
            BulkRecord bulkRecord = BulkRecord.bulkBuilder(REPO, MODEL, T1, Tester).hasApproval(true).objects(immutableList(
                simpleResult("&id", "aqua", "name", "Aqua"), simpleResult("&id", "beige", "name", "Beige"),
                simpleResult("&id", "cherry", "name", "Cherry"))).build();
            assertThat(indexer.strictSave(ES_AUTH, bulkRecord, bulkRecord.validator), is(true));
            checkStatusObjects(searcher, bool().liveIndex().build(), 0);
            checkStatusObjects(searcher, bool().pendingApproval().build(), 3);

            indexer.strictSave(ES_AUTH, approvePair(REPO, fetchApprovalDoc(searcher, ES_AUTH, REPO, MODEL, "aqua"), null, T2, Approver));
            checkStatusObjects(searcher, bool().liveIndex().build(), 1);
            indexer.strictSave(ES_AUTH, reject(REPO, fetchApprovalDoc(searcher, ES_AUTH, REPO, MODEL, "beige"),
                REJECT, null, T3, Approver), STRICT);
            assertNotNull(fetchPendingDoc(searcher, ES_AUTH, REPO, MODEL, "beige", bool().pendingOrRejected()));
            indexer.strictSave(ES_AUTH, discard(REPO, fetchApprovalDoc(searcher, ES_AUTH, REPO, MODEL, "cherry"),
                DISCARD, null, T4, Approver), STRICT);
            checkStatusObjects(searcher, bool().pendingApproval().build(), 0);
            checkStatusObjects(searcher, bool().discarded().build(), 1);

            String actual = checkStatusObjects(searcher, bool().allIndex().build(), 5);
            assertThat(actual, actual, is(MESSAGES.get("approval.one")));

        } finally { client.dropIndex(index2); }
    }

    @Test
    public void approvalStrictFailure() {
        String index2 = client.createIndex(indexFile(with(3, 0), typeBuilder().build(), REPO));
        try {
            indexer.strictSave(ES_AUTH, createRecord(REPO, approvalModel(MODEL), "neon", simpleResult("name", "Neon1"))
                .status(PENDING).userTs(Tester, T1).build(), STRICT);
            indexer.strictSave(ES_AUTH, createRecord(REPO, approvalModel(MODEL), "neon", simpleResult("name", "Neon2"))
                .status(PENDING).userTs(Tester, T2).build(), STRICT);
            fail("cannot come here");
        } catch (IndexerException iex) {
            assertThat(iex.getMessage(), is("Entity$approval:neon - record already exists"));
        } finally { client.dropIndex(index2); }
    }

    private static String checkStatusObjects(Searcher searcher, Expression expr, int results) {
        SearchRequest searchRequest = filterToRequest(REPO, expr, FETCH_SOURCE,
            immutableList(fieldSort(ID_FIELD), fieldSort(TIMESTAMP_FIELD).order(DESC)), 10);
        List<MapResult> mapResults = searcher.searchBy(ES_AUTH, searchRequest, SourceBuilder, META).resultsAs(MapResult.class);
        assertThat(mapResults.size(), is(results));
        return serialize(mapResults);
    }
}