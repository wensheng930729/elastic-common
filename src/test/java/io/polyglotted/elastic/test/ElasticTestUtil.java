package io.polyglotted.elastic.test;

import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.EsAuth;

import static io.polyglotted.elastic.client.ElasticSettings.elasticSettings;
import static io.polyglotted.elastic.client.HighLevelConnector.highLevelClient;
import static io.polyglotted.elastic.common.EsAuth.basicAuth;

abstract class ElasticTestUtil {
    static final EsAuth ES_AUTH = basicAuth("elastic", "SteelEye");

    static ElasticClient testElasticClient() {
        return highLevelClient(elasticSettings().setBootstrap("elastic", "SteelEye").setInsecure(true)).waitForYellow();
    }
}