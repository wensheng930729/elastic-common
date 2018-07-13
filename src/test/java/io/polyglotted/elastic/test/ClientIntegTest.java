package io.polyglotted.elastic.test;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.index.Indexer;
import io.polyglotted.elastic.search.Searcher;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.common.util.MapRetriever.deepRetrieve;
import static io.polyglotted.common.util.ResourceUtil.readResource;
import static io.polyglotted.elastic.admin.Field.keywordField;
import static io.polyglotted.elastic.admin.Field.simpleField;
import static io.polyglotted.elastic.admin.FieldType.INTEGER;
import static io.polyglotted.elastic.admin.IndexRequestor.templateFile;
import static io.polyglotted.elastic.admin.IndexSetting.with;
import static io.polyglotted.elastic.admin.Type.typeBuilder;
import static io.polyglotted.elastic.common.Verbose.NONE;
import static io.polyglotted.elastic.index.IndexRecord.createRecord;
import static io.polyglotted.elastic.search.QueryMaker.copyFrom;
import static io.polyglotted.elastic.search.ResultBuilder.SourceBuilder;
import static io.polyglotted.elastic.test.ElasticTestUtil.ES_AUTH;
import static io.polyglotted.elastic.test.ElasticTestUtil.testElasticClient;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ClientIntegTest {
    @Test
    public void testHighLevelClient() {
        checkClusterHealth();
        checkCreateDeleteIndex();
        checkPipelineLifecycle();
        checkTemplateLifecycle();
    }

    private static void checkClusterHealth() {
        try (ElasticClient client = testElasticClient()) {
            assertHealth(client.clusterHealth());
        }
    }

    static void assertHealth(MapResult health) {
        assertThat(health.keySet(), containsInAnyOrder("cluster_name", "status", "timed_out", "number_of_nodes", "number_of_data_nodes",
            "active_primary_shards", "active_shards", "relocating_shards", "initializing_shards", "unassigned_shards", "delayed_unassigned_shards",
            "number_of_pending_tasks", "number_of_in_flight_fetch", "task_max_waiting_in_queue_millis", "active_shards_percent_as_number"));
    }

    private static void checkCreateDeleteIndex() {
        String index = "customer";
        try (ElasticClient client = testElasticClient()) {
            assertThat(client.indexExists(index), is(false));
            client.createIndex(new CreateIndexRequest(index)
                .source(readResource(ClientIntegTest.class, "index-source.json"), XContentType.JSON));
            assertThat(client.indexExists(index), is(true));
            client.dropIndex(index);
            assertThat(client.indexExists(index), is(false));
        }
    }

    private void checkPipelineLifecycle() {
        String pipeline = "mypipe";
        try (ElasticClient client = testElasticClient()) {
            assertThat(client.pipelineExists(pipeline), is(false));
            client.putPipeline(pipeline, readResource(ClientIntegTest.class, "pipeline-source.json"));
            assertThat(client.pipelineExists(pipeline), is(true));
            client.deletePipeline(pipeline);
            assertThat(client.pipelineExists(pipeline), is(false));
        }
    }

    private void checkTemplateLifecycle() {
        String template = "1234absdffew234";
        try (ElasticClient client = testElasticClient()) {
            assertThat(client.templateExists(template), is(false));
            client.putTemplate(template, templateFile(with(1, 0), typeBuilder().strict().field(keywordField("hostName")).build(), "tpl-*"));
            assertThat(client.templateExists(template), is(true));
            client.putTemplate(template, templateFile(with(1, 0), typeBuilder().strict().field(keywordField("hostName"))
                .field(simpleField("createdAt", INTEGER)).build(), "tpl-*"));

            try {
                new Indexer(client).bulkSave(ES_AUTH, createRecord("tpl-2018", "Host", "bobcat",
                    simpleResult("hostName", "Bobcat", "createdAt", 10)).userTs("Tester", 10000L).build());
                MapResult mapResult = deserialize(new Searcher(client).searchNative(ES_AUTH, copyFrom("tpl-2018",
                    "{\"size\":10}".getBytes(UTF_8), null, NONE), SourceBuilder, false, NONE));
                assertThat(deepRetrieve(mapResult, "header.totalHits"), is(1));
            } finally { client.dropIndex("tpl-2018"); }
            client.deleteTemplate(template);
            assertThat(client.templateExists(template), is(false));
        }
    }
}