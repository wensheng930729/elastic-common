package io.polyglotted.elastic.test;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.elastic.client.ElasticClient;

import static io.polyglotted.common.model.AuthHeader.basicAuth;
import static io.polyglotted.elastic.client.ElasticSettings.elasticSettings;
import static io.polyglotted.elastic.client.HighLevelConnector.highLevelClient;

abstract class ElasticTestUtil {
    static final AuthHeader ES_AUTH = basicAuth("elastic", "SteelEye");

    static ElasticClient testElasticClient() {
        return highLevelClient(elasticSettings().setBootstrap("elastic", "SteelEye").setInsecure(true)).waitForYellow();
    }
}