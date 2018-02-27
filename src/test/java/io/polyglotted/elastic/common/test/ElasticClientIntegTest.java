package io.polyglotted.elastic.common.test;

import io.polyglotted.elastic.common.ElasticClient;
import org.junit.Test;

import java.util.Map;

import static io.polyglotted.elastic.common.ElasticSettings.esSettingsBuilder;
import static io.polyglotted.elastic.common.HighLevelConnector.highLevelClient;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class ElasticClientIntegTest {

    @Test
    public void testHighLevelClient() throws Exception {
        try (ElasticClient elasticClient = highLevelClient(esSettingsBuilder().userName("elastic").password("SteelEye").build())) {
            checkClusterHealth(elasticClient);
        }
    }

    private static void checkClusterHealth(ElasticClient client) {
        Map<String, Object> health = client.clusterHealth();
        assertThat(health.keySet(), containsInAnyOrder("cluster_name", "status", "timed_out", "number_of_nodes", "number_of_data_nodes",
            "active_primary_shards", "active_shards", "relocating_shards", "initializing_shards", "unassigned_shards", "delayed_unassigned_shards",
            "number_of_pending_tasks", "number_of_in_flight_fetch", "task_max_waiting_in_queue_millis", "active_shards_percent_as_number"));
    }
}