package io.polyglotted.elastic.test;

import io.polyglotted.elastic.admin.IndexRequestor;
import io.polyglotted.elastic.admin.IndexSetting;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.junit.Test;

import java.util.Map;

import static io.polyglotted.common.model.MapResult.immutableResult;
import static io.polyglotted.common.util.ResourceUtil.readResourceAsMap;
import static io.polyglotted.elastic.admin.IndexSetting.settingBuilder;
import static io.polyglotted.elastic.test.AdminIntegTest.completeTypeMapping;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class IndexRequestorTest extends IndexRequestor {
    private static final Map<String, String> MESSAGES = readResourceAsMap(IndexRequestorTest.class, "index-requestor.txt");

    @Test
    public void indexFileSuccess() throws Exception {
        IndexSetting setting = settingBuilder(5, 1).all(immutableResult("mapping.total_fields.limit", 5000)).build();
        String actual = indexFile(setting, completeTypeMapping().build(), "MyBigIndex");
        assertThat(actual, actual, is(MESSAGES.get("indexFile")));
    }

    @Test
    public void templateFileSuccess() throws Exception {
        IndexSetting setting = settingBuilder(5, 1).all(immutableResult("mapping.total_fields.limit", 5000)).build();
        String actual = templateFile(setting, completeTypeMapping().build(), "bigindex-*");
        assertThat(actual, actual, is(MESSAGES.get("templateFile")));
        assertThat(new PutIndexTemplateRequest().source(actual, JSON), is(notNullValue()));
    }
}