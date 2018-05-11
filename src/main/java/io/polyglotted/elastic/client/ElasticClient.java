package io.polyglotted.elastic.client;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.model.MapResult.ImmutableResult;
import io.polyglotted.common.util.TokenUtil;
import io.polyglotted.elastic.admin.IndexSetting;
import io.polyglotted.elastic.admin.Type;
import io.polyglotted.elastic.common.EsAuth;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;

import java.io.Closeable;

import static io.polyglotted.common.util.MapRetriever.optStr;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

@SuppressWarnings("unused")
public interface ElasticClient extends Closeable {
    void close();

    EsAuth bootstrapAuth();

    default ElasticClient waitForYellow() { return waitForStatus("yellow"); }

    default ElasticClient waitForStatus(String status) { return waitForStatus(bootstrapAuth(), status); }

    ElasticClient waitForStatus(EsAuth auth, String status);

    default MapResult clusterHealth() { return clusterHealth(bootstrapAuth()); }

    MapResult clusterHealth(EsAuth auth);

    default boolean indexExists(String index) { return indexExists(bootstrapAuth(), index); }

    boolean indexExists(EsAuth auth, String index);

    default String createIndex(IndexSetting setting, Type type, String alias) { return createIndex(bootstrapAuth(), setting, type, alias); }

    default String createIndex(EsAuth auth, IndexSetting setting, Type type, String alias) {
        CreateIndexRequest request = createIndexRequest(nonNull(optStr(setting.mapResult, "index_name"), TokenUtil::uniqueToken))
            .updateAllTypes(true).settings(setting.createJson(), JSON).mapping(type.type, type.mappingJson(), JSON);
        if (alias != null) { request.alias(new Alias(alias)); }
        return createIndex(auth, request);
    }

    default String createIndex(CreateIndexRequest request) { return createIndex(bootstrapAuth(), request); }

    String createIndex(EsAuth auth, CreateIndexRequest request);

    default void dropIndex(String index) { dropIndex(bootstrapAuth(), index); }

    void dropIndex(EsAuth auth, String index);

    default void forceRefresh(String index) { forceRefresh(bootstrapAuth(), index); }

    void forceRefresh(EsAuth auth, String index);

    default String getSettings(String index) { return getSettings(bootstrapAuth(), index); }

    String getSettings(EsAuth auth, String index);

    default ImmutableResult getMapping(String index) { return getMapping(bootstrapAuth(), index); }

    ImmutableResult getMapping(EsAuth auth, String index);

    default void putPipeline(String id, String resource) { putPipeline(bootstrapAuth(), id, resource); }

    void putPipeline(EsAuth auth, String id, String resource);

    default boolean pipelineExists(String id) { return pipelineExists(bootstrapAuth(), id); }

    boolean pipelineExists(EsAuth auth, String id);

    default void deletePipeline(String id) { deletePipeline(bootstrapAuth(), id); }

    void deletePipeline(EsAuth auth, String id);

    default IndexResponse index(IndexRequest request) { return index(bootstrapAuth(), request); }

    IndexResponse index(EsAuth auth, IndexRequest request);

    default DeleteResponse delete(DeleteRequest request) { return delete(bootstrapAuth(), request); }

    DeleteResponse delete(EsAuth auth, DeleteRequest request);

    default BulkResponse bulk(BulkRequest bulk) { return bulk(bootstrapAuth(), bulk); }

    BulkResponse bulk(EsAuth auth, BulkRequest bulk);

    default void bulkAsync(BulkRequest bulkRequest, ActionListener<BulkResponse> listener) { bulkAsync(bootstrapAuth(), bulkRequest, listener); }

    void bulkAsync(EsAuth auth, BulkRequest bulkRequest, ActionListener<BulkResponse> listener);

    default boolean exists(GetRequest request) { return exists(bootstrapAuth(), request); }

    boolean exists(EsAuth auth, GetRequest request);

    default MultiGetResponse multiGet(MultiGetRequest request) { return multiGet(bootstrapAuth(), request); }

    MultiGetResponse multiGet(EsAuth auth, MultiGetRequest request);

    default SearchResponse search(SearchRequest request) { return search(bootstrapAuth(), request); }

    SearchResponse search(EsAuth auth, SearchRequest request);

    default SearchResponse searchScroll(SearchScrollRequest request) { return searchScroll(bootstrapAuth(), request); }

    SearchResponse searchScroll(EsAuth auth, SearchScrollRequest request);

    default ClearScrollResponse clearScroll(ClearScrollRequest request) { return clearScroll(bootstrapAuth(), request); }

    ClearScrollResponse clearScroll(EsAuth auth, ClearScrollRequest request);

    default MapResult xpackPut(XPackApi api, String id, String body) { return xpackPut(bootstrapAuth(), api, id, body); }

    MapResult xpackPut(EsAuth auth, XPackApi api, String id, String body);

    default MapResult xpackGet(XPackApi api, String id) { return xpackGet(bootstrapAuth(), api, id); }

    MapResult xpackGet(EsAuth auth, XPackApi api, String id);

    default void xpackDelete(XPackApi api, String id) { xpackDelete(bootstrapAuth(), api, id); }

    void xpackDelete(EsAuth auth, XPackApi api, String id);

    default void xpackDelete(XPackApi api, String id, String body) { xpackDelete(bootstrapAuth(), api, id, body); }

    void xpackDelete(EsAuth auth, XPackApi api, String id, String body);
}