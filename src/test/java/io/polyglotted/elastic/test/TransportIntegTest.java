package io.polyglotted.elastic.test;

import io.polyglotted.elastic.client.ElasticClient;
import org.junit.Test;

import static io.polyglotted.elastic.test.ClientIntegTest.assertHealth;
import static io.polyglotted.elastic.test.ElasticTestUtil.testTransportClient;

public class TransportIntegTest {
    @Test
    public void transportSuccess() {
        try (ElasticClient client = testTransportClient()) {
            assertHealth(client.clusterHealth());
        }
    }
}