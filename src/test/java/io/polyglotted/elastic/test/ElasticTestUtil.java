package io.polyglotted.elastic.test;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.client.ElasticTransportClient;
import lombok.SneakyThrows;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;

import static io.polyglotted.common.model.AuthHeader.basicAuth;
import static io.polyglotted.elastic.client.ElasticSettings.elasticSettings;
import static io.polyglotted.elastic.client.HighLevelConnector.highLevelClient;
import static org.elasticsearch.common.transport.TransportAddress.META_ADDRESS;

abstract class ElasticTestUtil {
    static final AuthHeader ES_AUTH = basicAuth("elastic", "SteelEye");

    static ElasticClient testElasticClient() {
        return highLevelClient(elasticSettings().setBootstrap("elastic", "SteelEye").setInsecure(false)).waitForYellow();
    }

    @SneakyThrows static ElasticClient testTransportClient() {
        TransportClient client = new PreBuiltXPackTransportClient(Settings.builder()
            .put("cluster.name", "steeleye-cluster")
            .put("xpack.security.user", "elastic:SteelEye")
            .put("xpack.security.transport.ssl.enabled", "true")
            .put("xpack.ssl.verification_mode", "none")
            .build())
            .addTransportAddress(new TransportAddress(META_ADDRESS, 9500));
        return new ElasticTransportClient(client, AuthHeader.basicAuth("elastic", "SteelEye")).waitForYellow();
    }
}