package io.polyglotted.elastic.test;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.admin.Admin;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.EsAuth;
import io.polyglotted.elastic.index.BulkRecord;
import io.polyglotted.elastic.index.Indexer;
import io.polyglotted.elastic.index.Validator;
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
import static io.polyglotted.elastic.admin.IndexSetting.with;
import static io.polyglotted.elastic.admin.Type.typeBuilder;
import static io.polyglotted.elastic.client.ElasticSettings.elasticSettings;
import static io.polyglotted.elastic.client.HighLevelConnector.highLevelClient;
import static io.polyglotted.elastic.common.EsAuth.basicAuth;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TIMESTAMP_FIELD;
import static io.polyglotted.elastic.common.Verbose.META;
import static io.polyglotted.elastic.index.ApprovalUtil.approvePair;
import static io.polyglotted.elastic.index.ApprovalUtil.discard;
import static io.polyglotted.elastic.index.ApprovalUtil.fetchApprovalDoc;
import static io.polyglotted.elastic.index.ApprovalUtil.reject;
import static io.polyglotted.elastic.index.RecordAction.DISCARD;
import static io.polyglotted.elastic.index.RecordAction.REJECT;
import static io.polyglotted.elastic.search.Expressions.bool;
import static io.polyglotted.elastic.search.QueryMaker.filterToRequest;
import static io.polyglotted.elastic.search.ResultBuilder.SourceBuilder;
import static org.elasticsearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;
import static org.elasticsearch.search.sort.SortOrder.DESC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ApprovalIntegTest {
    private static final Map<String, String> MESSAGES = readResourceAsMap(ApprovalIntegTest.class, "approval-integ.txt");
    private static final EsAuth ES_AUTH = basicAuth("elastic", "SteelEye");
    private static final String Tester = "tester", Approver = "approver";
    private static final long T1 = 1000, T2 = 2000, T3 = 3000;
    private static final String REPO = "AppSvc", MODEL = "Entity";
    private ElasticClient client;
    private Admin admin;
    private Indexer indexer;
    private Searcher searcher;

    @Before public void init() {
        client = highLevelClient(elasticSettings(), ES_AUTH);
        admin = new Admin(client); indexer = new Indexer(client); searcher = new Searcher(client);
    }

    @After public void close() { client.close(); }

    @Test
    public void approvalLifeCycle() throws Exception {
        String index2 = admin.createIndex(ES_AUTH, with(3, 0), typeBuilder().build(), REPO);
        try {
            BulkRecord bulkRecord = BulkRecord.bulkBuilder(REPO, MODEL, T1, Tester).hasApproval(true).objects(immutableList(
                simpleResult("&id", "aqua", "name", "Aqua"), simpleResult("&id", "beige", "name", "Beige"),
                simpleResult("&id", "cherry", "name", "Cherry"))).build();
            assertThat(indexer.strictSave(ES_AUTH, bulkRecord), is(true));
            checkStatusObjects(searcher, bool().liveIndex().build(), 0);
            checkStatusObjects(searcher, bool().pendingApproval().build(), 3);

            indexer.strictSave(ES_AUTH, approvePair(fetchApprovalDoc(searcher, ES_AUTH, REPO, MODEL, "aqua"), null, T2, Approver));
            checkStatusObjects(searcher, bool().liveIndex().build(), 1);
            indexer.strictSave(ES_AUTH, reject(fetchApprovalDoc(searcher, ES_AUTH, REPO, MODEL, "beige"),
                REJECT, null, T2, Approver), Validator.STRICT);
            checkStatusObjects(searcher, bool().rejected().build(), 1);
            indexer.strictSave(ES_AUTH, discard(fetchApprovalDoc(searcher, ES_AUTH, REPO, MODEL, "cherry"),
                DISCARD, null, T2, Approver), Validator.STRICT);
            checkStatusObjects(searcher, bool().pendingApproval().build(), 0);
            checkStatusObjects(searcher, bool().discarded().build(), 1);
            assertThat(checkStatusObjects(searcher, bool().allIndex().build(), 5), is(MESSAGES.get("approval.one")));

        } finally { admin.dropIndex(ES_AUTH, index2); }
    }

    private static String checkStatusObjects(Searcher searcher, Expression expr, int results) {
        SearchRequest searchRequest = filterToRequest(REPO, expr, FETCH_SOURCE,
            immutableList(fieldSort(ID_FIELD), fieldSort(TIMESTAMP_FIELD).order(DESC)), 10);
        List<MapResult> mapResults = searcher.searchBy(ES_AUTH, searchRequest, SourceBuilder, META).resultsAs(MapResult.class);
        assertThat(mapResults.size(), is(results));
        return serialize(mapResults);
    }
}