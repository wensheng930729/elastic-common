package io.polyglotted.elastic.test;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.MapBuilder;
import io.polyglotted.elastic.admin.Admin;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.DocResult;
import io.polyglotted.elastic.common.EsAuth;
import io.polyglotted.elastic.index.IndexRecord;
import io.polyglotted.elastic.index.Indexer;
import io.polyglotted.elastic.index.IndexerException;
import io.polyglotted.elastic.index.Validator.StrictValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.util.MapBuilder.immutableMap;
import static io.polyglotted.common.util.MapBuilder.immutableMapBuilder;
import static io.polyglotted.elastic.admin.IndexSetting.with;
import static io.polyglotted.elastic.admin.Type.typeBuilder;
import static io.polyglotted.elastic.client.ElasticSettings.elasticSettings;
import static io.polyglotted.elastic.client.HighLevelConnector.highLevelClient;
import static io.polyglotted.elastic.common.EsAuth.basicAuth;
import static io.polyglotted.elastic.common.MetaFields.ANCESTOR_FIELD;
import static io.polyglotted.elastic.common.MetaFields.EXPIRY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.MODEL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.STATUS_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TIMESTAMP_FIELD;
import static io.polyglotted.elastic.common.MetaFields.UPDATER_FIELD;
import static io.polyglotted.elastic.common.MetaFields.USER_FIELD;
import static io.polyglotted.elastic.common.MetaFields.simpleKey;
import static io.polyglotted.elastic.index.IndexRecord.createRecord;
import static io.polyglotted.elastic.index.IndexRecord.deleteRecord;
import static io.polyglotted.elastic.index.IndexRecord.expired;
import static io.polyglotted.elastic.index.IndexRecord.updateRecord;
import static io.polyglotted.elastic.index.RecordAction.UPDATE;
import static io.polyglotted.elastic.index.Validator.OVERRIDE;
import static io.polyglotted.elastic.index.Validator.STRICT;
import static io.polyglotted.elastic.search.Finder.findById;
import static io.polyglotted.elastic.search.Finder.findByKey;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

public class IndexerIntegTest {
    private static final EsAuth ES_AUTH = basicAuth("elastic", "SteelEye");
    private static final String Tester = "tester";
    private static final long T1 = 1000, T1_5 = 1500, T2 = 2000, T3 = 3000;
    private static final String REPO = "repo1", MODEL = "User", ID = "sam";
    private ElasticClient client;
    private Admin admin;
    private Indexer indexer;

    @Before public void init() {
        client = highLevelClient(elasticSettings(), ES_AUTH); admin = new Admin(client); indexer = new Indexer(client);
    }

    @After public void close() { client.close(); }

    @Test
    public void strictSaveLifeCycle() throws Exception {
        String index2 = admin.createIndex(ES_AUTH, with(3, 0), typeBuilder().build(), REPO);
        try {
            MapResult user = simpleResult("name", "shankar", "age", 25, "title", "programmer");
            String result = indexer.strictSave(ES_AUTH, createRecord(REPO, MODEL, ID, user).userTs(Tester, T1).build(), STRICT);
            assertThat(result, result, is("{\"&model\":\"User\",\"&id\":\"sam\",\"&timestamp\":1000,\"&result\":\"created\"}"));
            assertHeaders(findById(client, ES_AUTH, REPO, MODEL, ID), immutableMap(MODEL_FIELD, MODEL, ID_FIELD, ID,
                TIMESTAMP_FIELD, String.valueOf(T1), USER_FIELD, Tester));

            result = indexer.strictSave(ES_AUTH, createRecord(REPO, MODEL, ID, user).userTs(Tester, T1_5).build(), STRICT);
            assertThat(result, result, is("{\"&model\":\"User\",\"&id\":\"sam\",\"&timestamp\":1000,\"&result\":\"noop\"}"));

            MapResult user2 = simpleResult("name", "shankar", "age", 25, "title", "developer", "salary", 10000L);
            result = indexer.strictSave(ES_AUTH, updateRecord(REPO, MODEL, ID, T1, user2).userTs(Tester, T2).build(), STRICT);
            assertThat(result, result, is("{\"&model\":\"User\",\"&id\":\"sam\",\"&timestamp\":2000,\"&result\":\"updated\"}"));
            String ancestor1 = simpleKey(MODEL, null, ID, T1);
            assertHeaders(findById(client, ES_AUTH, REPO, MODEL, ID), stringMapBuilder().put(MODEL_FIELD, MODEL)
                .put(ID_FIELD, ID).put(TIMESTAMP_FIELD, String.valueOf(T2)).put(USER_FIELD, Tester).put(ANCESTOR_FIELD, ancestor1).build());
            assertHeaders(findByKey(client, ES_AUTH, REPO, ancestor1), stringMapBuilder().put(MODEL_FIELD, MODEL)
                .put(ID_FIELD, ID).put(TIMESTAMP_FIELD, String.valueOf(T1)).put(USER_FIELD, Tester).put(UPDATER_FIELD, Tester)
                .put(EXPIRY_FIELD, String.valueOf(T2)).put(STATUS_FIELD, "UPDATED").build());

            result = indexer.strictSave(ES_AUTH, deleteRecord(REPO, MODEL, ID, T2).userTs(Tester, T3).build(), STRICT);
            assertThat(result, result, is("{\"&model\":\"User\",\"&id\":\"sam\",\"&timestamp\":3000,\"&result\":\"deleted\"}"));
            assertThat(findById(client, ES_AUTH, REPO, MODEL, ID), is(nullValue()));
            String ancestor2 = simpleKey(MODEL, null, ID, T2);
            assertHeaders(findByKey(client, ES_AUTH, REPO, ancestor2), stringMapBuilder().put(MODEL_FIELD, MODEL)
                .put(ID_FIELD, ID).put(TIMESTAMP_FIELD, String.valueOf(T2)).put(USER_FIELD, Tester).put(UPDATER_FIELD, Tester)
                .put(EXPIRY_FIELD, String.valueOf(T3)).put(STATUS_FIELD, "DELETED").build());

        } finally { admin.dropIndex(ES_AUTH, index2); }
    }

    @Test
    public void overrideSaveLifeCycle() {
        String index2 = admin.createIndex(ES_AUTH, with(3, 0), typeBuilder().build(), REPO);
        try {
            MapResult user = simpleResult("name", "shankar", "age", 25, "title", "programmer");
            String result = indexer.strictSave(ES_AUTH, createRecord(REPO, MODEL, ID, user).userTs(Tester, T1).build(), STRICT);
            assertThat(result, result, is("{\"&model\":\"User\",\"&id\":\"sam\",\"&timestamp\":1000,\"&result\":\"created\"}"));
            assertHeaders(findById(client, ES_AUTH, REPO, MODEL, ID), immutableMap(MODEL_FIELD, MODEL, ID_FIELD, ID,
                TIMESTAMP_FIELD, String.valueOf(T1), USER_FIELD, Tester));

            result = indexer.strictSave(ES_AUTH, createRecord(REPO, MODEL, ID, user).userTs(Tester, T1_5).build(), STRICT);
            assertThat(result, result, is("{\"&model\":\"User\",\"&id\":\"sam\",\"&timestamp\":1000,\"&result\":\"noop\"}"));

            MapResult user2 = simpleResult("name", "shankar", "age", 25, "title", "developer", "salary", 10000L);
            result = indexer.strictSave(ES_AUTH, createRecord(REPO, MODEL, ID, user2).userTs(Tester, T2).build(), OVERRIDE);
            assertThat(result, result, is("{\"&model\":\"User\",\"&id\":\"sam\",\"&timestamp\":2000,\"&result\":\"updated\"}"));
            String ancestor1 = simpleKey(MODEL, null, ID, T1);
            assertHeaders(findById(client, ES_AUTH, REPO, MODEL, ID), stringMapBuilder().put(MODEL_FIELD, MODEL)
                .put(ID_FIELD, ID).put(TIMESTAMP_FIELD, String.valueOf(T2)).put(USER_FIELD, Tester).put(ANCESTOR_FIELD, ancestor1).build());
            assertHeaders(findByKey(client, ES_AUTH, REPO, ancestor1), stringMapBuilder().put(MODEL_FIELD, MODEL)
                .put(ID_FIELD, ID).put(TIMESTAMP_FIELD, String.valueOf(T1)).put(USER_FIELD, Tester).put(UPDATER_FIELD, Tester)
                .put(EXPIRY_FIELD, String.valueOf(T2)).put(STATUS_FIELD, "UPDATED").build());

            result = indexer.strictSave(ES_AUTH, deleteRecord(REPO, MODEL, ID, null).userTs(Tester, T3).build(), OVERRIDE);
            assertThat(result, result, is("{\"&model\":\"User\",\"&id\":\"sam\",\"&timestamp\":3000,\"&result\":\"deleted\"}"));
            assertThat(findById(client, ES_AUTH, REPO, MODEL, ID), is(nullValue()));
            String ancestor2 = simpleKey(MODEL, null, ID, T2);
            assertHeaders(findByKey(client, ES_AUTH, REPO, ancestor2), stringMapBuilder().put(MODEL_FIELD, MODEL)
                .put(ID_FIELD, ID).put(TIMESTAMP_FIELD, String.valueOf(T2)).put(USER_FIELD, Tester).put(UPDATER_FIELD, Tester)
                .put(EXPIRY_FIELD, String.valueOf(T3)).put(STATUS_FIELD, "DELETED").build());

        } finally { admin.dropIndex(ES_AUTH, index2); }
    }

    @Test
    public void strictSaveFailureAlreadyExists() {
        String index2 = admin.createIndex(ES_AUTH, with(3, 0), typeBuilder().build(), REPO);
        try {
            indexer.strictSave(ES_AUTH, createRecord(REPO, MODEL, ID, simpleResult("name", "shankar", "age", 25,
                "title", "programmer")).userTs(Tester, T1).build(), STRICT);
            indexer.strictSave(ES_AUTH, createRecord(REPO, MODEL, ID, simpleResult("name", "shankar", "age", 25,
                "title", "developer", "salary", 10000L)).userTs(Tester, T2).build(), STRICT);
            fail("cannot come here");
        } catch (IndexerException iex) {
            assertThat(iex.getMessage(), is("User:sam - record already exists"));
        } finally { admin.dropIndex(ES_AUTH, index2); }
    }

    @Test
    public void strictSaveFailureDeleteNonExistent() {
        String index2 = admin.createIndex(ES_AUTH, with(3, 0), typeBuilder().build(), REPO);
        try {
            indexer.strictSave(ES_AUTH, deleteRecord(REPO, MODEL, ID, T2).userTs(Tester, T3).build(), STRICT);
            fail("cannot come here");
        } catch (IndexerException iex) {
            assertThat(iex.getMessage(), is("User:sam - record not found for update"));
        } finally { admin.dropIndex(ES_AUTH, index2); }
    }

    @Test
    public void strictSaveFailureBaseVersionNotFound() {
        String index2 = admin.createIndex(ES_AUTH, with(3, 0), typeBuilder().build(), REPO);
        try {
            indexer.strictSave(ES_AUTH, createRecord(REPO, MODEL, ID, simpleResult("name", "shankar", "age", 25,
                "title", "programmer")).userTs(Tester, T1).build(), STRICT);
            indexer.strictSave(ES_AUTH, expired(UPDATE, REPO, MODEL, ID, null, null, simpleResult("name", "shankar", "age", 25,
                "title", "developer", "salary", 10000L)).userTs(Tester, T2).build(), STRICT);
            fail("cannot come here");
        } catch (IndexerException iex) {
            assertThat(iex.getMessage(), is("User:sam - baseVersion not found for update"));
        } finally { admin.dropIndex(ES_AUTH, index2); }
    }

    @Test
    public void strictSaveFailureVersionMismatch() {
        String index2 = admin.createIndex(ES_AUTH, with(3, 0), typeBuilder().build(), REPO);
        try {
            indexer.strictSave(ES_AUTH, createRecord(REPO, MODEL, ID, simpleResult("name", "shankar", "age", 25,
                "title", "programmer")).userTs(Tester, T1).build(), STRICT);
            indexer.strictSave(ES_AUTH, updateRecord(REPO, MODEL, ID, T1, simpleResult("name", "shankar", "age", 25,
                "title", "developer", "salary", 10000L)).userTs(Tester, T2).build(), STRICT);
            indexer.strictSave(ES_AUTH, updateRecord(REPO, MODEL, ID, T1, simpleResult("name", "shankar", "age", 25,
                "title", "sr developer")).userTs(Tester, T3).build(), STRICT);
            fail("cannot come here");
        } catch (IndexerException iex) {
            assertThat(iex.getMessage(), is("User:sam - version conflict for update"));
        } finally { admin.dropIndex(ES_AUTH, index2); }
    }

    @Test
    public void failedValidation() {
        String index2 = admin.createIndex(ES_AUTH, with(3, 0), typeBuilder().build(), REPO);
        try {
            indexer.strictSave(ES_AUTH, createRecord(REPO, MODEL, ID, simpleResult()).userTs(Tester, T1).build(), new StrictValidator() {
                @Override protected void preValidate(ElasticClient client, IndexRecord record) { throw new RuntimeException("induced validation"); }
            });
            fail("cannot come here");
        } catch (IndexerException ie) {
            assertThat(ie.getMessage(), is("induced validation"));
        } finally { admin.dropIndex(ES_AUTH, index2); }
    }

    private static void assertHeaders(DocResult result, Map<String, String> headers) {
        assertThat(result, is(notNullValue())); assertThat(result.source, is(notNullValue()));
        headers.forEach((key, value) -> assertThat(result.source.get(key), is(value)));
    }

    private static MapBuilder.ImmutableMapBuilder<String, String> stringMapBuilder() { return immutableMapBuilder(); }
}