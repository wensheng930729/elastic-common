package io.polyglotted.elastic.test;

import io.polyglotted.elastic.common.ElasticClient;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.util.Map;

import static com.google.common.base.Charsets.UTF_8;
import static io.polyglotted.common.util.ResourceUtil.readResource;
import static io.polyglotted.elastic.common.ElasticSettings.esSettingsBuilder;
import static io.polyglotted.elastic.common.HighLevelConnector.highLevelClient;
import static org.apache.commons.codec.binary.Base64.encodeBase64String;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ElasticClientIntegTest {
    private static final String AUTH = "Basic " + encodeBase64String("elastic:SteelEye".getBytes(UTF_8));

    @Test
    public void testHighLevelClient() throws Exception {
        try (ElasticClient client = highLevelClient(esSettingsBuilder().userName("elastic").password("SteelEye").build())) {
            client.waitForStatus("yellow");
        }
        checkClusterHealth();
        checkCreateDeleteIndex();
        checkPipelineLifecycle();
    }

    private static void checkClusterHealth() throws Exception {
        try (ElasticClient client = highLevelClient(esSettingsBuilder().build())) {
            client.waitForStatus("yellow", AUTH);
            Map<String, Object> health = client.clusterHealth(AUTH);
            assertThat(health.keySet(), containsInAnyOrder("cluster_name", "status", "timed_out", "number_of_nodes", "number_of_data_nodes",
                "active_primary_shards", "active_shards", "relocating_shards", "initializing_shards", "unassigned_shards", "delayed_unassigned_shards",
                "number_of_pending_tasks", "number_of_in_flight_fetch", "task_max_waiting_in_queue_millis", "active_shards_percent_as_number"));
        }
    }

    private static void checkCreateDeleteIndex() throws Exception {
        String index = "customer";
        try (ElasticClient client = highLevelClient(esSettingsBuilder().build())) {
            assertThat(client.indexExists(index, AUTH), is(false));
            client.createIndex(new CreateIndexRequest(index)
                .source(readResource(ElasticClientIntegTest.class, "index-source.json"), XContentType.JSON), AUTH);
            assertThat(client.indexExists(index, AUTH), is(true));
            client.dropIndex(index, AUTH);
            assertThat(client.indexExists(index, AUTH), is(false));
        }
    }

    private void checkPipelineLifecycle() throws Exception {
        String pipeline = "mypipe";
        try (ElasticClient client = highLevelClient(esSettingsBuilder().build())) {
            assertThat(client.pipelineExists(pipeline, AUTH), is(false));
            client.buildPipeline(pipeline, readResource(ElasticClientIntegTest.class, "pipeline-source.json"), AUTH);
            assertThat(client.pipelineExists(pipeline, AUTH), is(true));
            client.deletePipeline(pipeline, AUTH);
            assertThat(client.pipelineExists(pipeline, AUTH), is(false));
        }
    }
}