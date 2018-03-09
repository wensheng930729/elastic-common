package io.polyglotted.elastic.admin;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.model.MapResult.ImmutableResult;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.EsAuth;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;

import java.math.BigInteger;
import java.security.SecureRandom;

import static io.polyglotted.common.util.MapRetriever.optStr;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

@SuppressWarnings({"unused", "WeakerAccess"})
@Slf4j @RequiredArgsConstructor
public final class Admin implements AutoCloseable {
    private static final SecureRandom random = new SecureRandom();

    @Delegate(types = AdminClient.class) private final ElasticClient client;

    public String createIndex(EsAuth auth, IndexSetting setting, Type type, String alias) {
        CreateIndexRequest request = createIndexRequest(nonNull(optStr(setting.mapResult, "index_name"), Admin::uniqueIndexName))
            .updateAllTypes(true).settings(setting.createJson(), JSON).mapping(type.type, type.mappingJson(), JSON);
        if (alias != null) { request.alias(new Alias(alias)); }
        return createIndex(auth, request);
    }

    private static String uniqueIndexName() { return (new BigInteger(130, random)).toString(32).toLowerCase(); }

    //@Formatter:off
    private interface AdminClient {
        void close();
        boolean indexExists(EsAuth auth, String index);
        String getSettings(EsAuth auth, String index);
        ImmutableResult getMapping(EsAuth auth, String index);
        String createIndex(EsAuth auth, CreateIndexRequest request);
        void dropIndex(EsAuth auth, String index);
        void waitForStatus(EsAuth auth, String status);
        MapResult clusterHealth(EsAuth auth);
        void buildPipeline(EsAuth auth, String id, String resource);
        boolean pipelineExists(EsAuth auth, String id);
    } //@Formatter:on
}