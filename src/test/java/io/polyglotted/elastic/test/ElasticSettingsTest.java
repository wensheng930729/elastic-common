package io.polyglotted.elastic.test;

import io.polyglotted.elastic.client.ElasticSettings;
import org.junit.Test;

import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.common.util.BaseSerializer.serialize;
import static io.polyglotted.elastic.client.ElasticSettings.elasticSettings;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ElasticSettingsTest {
    @Test
    public void settingsFromJson() {
        ElasticSettings expected = elasticSettings().setPort(19200).setBootstrap("elastic", "testMe");
        String json = "{\"port\":19200,\"bootstrap\":{\"username\":\"elastic\",\"password\":\"testMe\"}}";
        ElasticSettings actual = deserialize(json, ElasticSettings.class);
        assertThat(serialize(actual), serialize(actual), is(serialize(expected)));
    }
}