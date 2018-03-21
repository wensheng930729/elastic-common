package io.polyglotted.elastic.test;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.EsAuth;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import static io.polyglotted.common.util.ResourceUtil.readResource;
import static io.polyglotted.elastic.client.ElasticSettings.elasticSettings;
import static io.polyglotted.elastic.client.HighLevelConnector.highLevelClient;
import static io.polyglotted.elastic.common.EsAuth.AuthType.BASIC;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ClientIntegTest {
    private static final EsAuth ES_AUTH = new EsAuth("elastic", "SteelEye", BASIC);

    @Test
    public void testHighLevelClient() throws Exception {
        checkClusterHealth();
        checkCreateDeleteIndex();
        checkPipelineLifecycle();
    }

    private static void checkClusterHealth() throws Exception {
        try (ElasticClient client = highLevelClient(elasticSettings())) {
            MapResult health = client.clusterHealth(ES_AUTH);
            assertThat(health.keySet(), containsInAnyOrder("cluster_name", "status", "timed_out", "number_of_nodes", "number_of_data_nodes",
                "active_primary_shards", "active_shards", "relocating_shards", "initializing_shards", "unassigned_shards", "delayed_unassigned_shards",
                "number_of_pending_tasks", "number_of_in_flight_fetch", "task_max_waiting_in_queue_millis", "active_shards_percent_as_number"));
        }
    }

    private static void checkCreateDeleteIndex() throws Exception {
        String index = "customer";
        try (ElasticClient client = highLevelClient(elasticSettings())) {
            assertThat(client.indexExists(ES_AUTH, index), is(false));
            client.createIndex(ES_AUTH, new CreateIndexRequest(index)
                .source(readResource(ClientIntegTest.class, "index-source.json"), XContentType.JSON));
            assertThat(client.indexExists(ES_AUTH, index), is(true));
            client.dropIndex(ES_AUTH, index);
            assertThat(client.indexExists(ES_AUTH, index), is(false));
        }
    }

    private void checkPipelineLifecycle() throws Exception {
        String pipeline = "mypipe";
        try (ElasticClient client = highLevelClient(elasticSettings())) {
            assertThat(client.pipelineExists(ES_AUTH, pipeline), is(false));
            client.putPipeline(ES_AUTH, pipeline, readResource(ClientIntegTest.class, "pipeline-source.json"));
            assertThat(client.pipelineExists(ES_AUTH, pipeline), is(true));
            client.deletePipeline(ES_AUTH, pipeline);
            assertThat(client.pipelineExists(ES_AUTH, pipeline), is(false));
        }
    }
}