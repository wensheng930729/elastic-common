package io.polyglotted.elastic.admin;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.EsAuth;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.UUIDs;

import java.util.List;

import static io.polyglotted.common.util.MapRetriever.optStr;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

@SuppressWarnings({"unused", "WeakerAccess"})
@Slf4j @RequiredArgsConstructor
public final class Admin {
    @Delegate(types = AdminClient.class) private final ElasticClient client;

    public String createIndex(EsAuth auth, IndexSetting setting, List<Type> types, String... aliases) {
        String index = nonNull(optStr(setting.mapResult, "index_name"), () -> UUIDs.base64UUID().toLowerCase());
        CreateIndexRequest request = createIndexRequest(index).updateAllTypes(true).settings(setting.createJson(), JSON);
        for (String alias : aliases) { request.alias(new Alias(alias)); }
        for (Type type : types) { request.mapping(type.type, type.mappingJson(), JSON); }
        return createIndex(auth, request);
    }

    //@Formatter:off
    private interface AdminClient {
        boolean indexExists(EsAuth auth, String index);
        String getSettings(EsAuth auth, String index);
        String getMapping(EsAuth auth, String index);
        String createIndex(EsAuth auth, CreateIndexRequest request);
        void dropIndex(EsAuth auth, String index);
        void waitForStatus(EsAuth auth, String status);
        MapResult clusterHealth(EsAuth auth);
        void buildPipeline(EsAuth auth, String id, String resource);
        boolean pipelineExists(EsAuth auth, String id);
    } //@Formatter:on
}