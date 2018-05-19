package io.polyglotted.elastic.client;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.model.MapResult.ImmutableResult;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.common.xcontent.XContentType;

import java.io.Closeable;

import static io.polyglotted.common.util.TokenUtil.uniqueToken;

@SuppressWarnings("unused")
public interface ElasticClient extends Closeable {
    void close();

    AuthHeader bootstrapAuth();

    default ElasticClient waitForYellow() { return waitForStatus("yellow"); }

    default ElasticClient waitForStatus(String status) { return waitForStatus(bootstrapAuth(), status); }

    ElasticClient waitForStatus(AuthHeader auth, String status);

    default MapResult clusterHealth() { return clusterHealth(bootstrapAuth()); }

    MapResult clusterHealth(AuthHeader auth);

    default boolean indexExists(String index) { return indexExists(bootstrapAuth(), index); }

    boolean indexExists(AuthHeader auth, String index);

    default String createIndex(String indexFile) { return createIndex(uniqueToken(), indexFile); }

    default String createIndex(String indexName, String indexFile) { return createIndex(bootstrapAuth(), indexName, indexFile); }

    default String createIndex(AuthHeader auth, String indexName, String indexFile) {
        return createIndex(auth, new CreateIndexRequest(indexName).source(indexFile, XContentType.JSON));
    }

    default String createIndex(CreateIndexRequest request) { return createIndex(bootstrapAuth(), request); }

    String createIndex(AuthHeader auth, CreateIndexRequest request);

    default void dropIndex(String index) { dropIndex(bootstrapAuth(), index); }

    void dropIndex(AuthHeader auth, String index);

    default void forceRefresh(String index) { forceRefresh(bootstrapAuth(), index); }

    void forceRefresh(AuthHeader auth, String index);

    default String getSettings(String index) { return getSettings(bootstrapAuth(), index); }

    String getSettings(AuthHeader auth, String index);

    default ImmutableResult getMapping(String index) { return getMapping(bootstrapAuth(), index); }

    ImmutableResult getMapping(AuthHeader auth, String index);

    default void putPipeline(String id, String resource) { putPipeline(bootstrapAuth(), id, resource); }

    void putPipeline(AuthHeader auth, String id, String resource);

    default boolean pipelineExists(String id) { return pipelineExists(bootstrapAuth(), id); }

    boolean pipelineExists(AuthHeader auth, String id);

    default void deletePipeline(String id) { deletePipeline(bootstrapAuth(), id); }

    void deletePipeline(AuthHeader auth, String id);

    default IndexResponse index(IndexRequest request) { return index(bootstrapAuth(), request); }

    IndexResponse index(AuthHeader auth, IndexRequest request);

    default DeleteResponse delete(DeleteRequest request) { return delete(bootstrapAuth(), request); }

    DeleteResponse delete(AuthHeader auth, DeleteRequest request);

    default BulkResponse bulk(BulkRequest bulk) { return bulk(bootstrapAuth(), bulk); }

    BulkResponse bulk(AuthHeader auth, BulkRequest bulk);

    default void bulkAsync(BulkRequest bulkRequest, ActionListener<BulkResponse> listener) { bulkAsync(bootstrapAuth(), bulkRequest, listener); }

    void bulkAsync(AuthHeader auth, BulkRequest bulkRequest, ActionListener<BulkResponse> listener);

    default boolean exists(GetRequest request) { return exists(bootstrapAuth(), request); }

    boolean exists(AuthHeader auth, GetRequest request);

    default MultiGetResponse multiGet(MultiGetRequest request) { return multiGet(bootstrapAuth(), request); }

    MultiGetResponse multiGet(AuthHeader auth, MultiGetRequest request);

    default SearchResponse search(SearchRequest request) { return search(bootstrapAuth(), request); }

    SearchResponse search(AuthHeader auth, SearchRequest request);

    default SearchResponse searchScroll(SearchScrollRequest request) { return searchScroll(bootstrapAuth(), request); }

    SearchResponse searchScroll(AuthHeader auth, SearchScrollRequest request);

    default ClearScrollResponse clearScroll(ClearScrollRequest request) { return clearScroll(bootstrapAuth(), request); }

    ClearScrollResponse clearScroll(AuthHeader auth, ClearScrollRequest request);

    default MapResult xpackPut(XPackApi api, String id, String body) { return xpackPut(bootstrapAuth(), api, id, body); }

    MapResult xpackPut(AuthHeader auth, XPackApi api, String id, String body);

    default MapResult xpackGet(XPackApi api, String id) { return xpackGet(bootstrapAuth(), api, id); }

    MapResult xpackGet(AuthHeader auth, XPackApi api, String id);

    default void xpackDelete(XPackApi api, String id) { xpackDelete(bootstrapAuth(), api, id); }

    void xpackDelete(AuthHeader auth, XPackApi api, String id);

    default void xpackDelete(XPackApi api, String id, String body) { xpackDelete(bootstrapAuth(), api, id, body); }

    void xpackDelete(AuthHeader auth, XPackApi api, String id, String body);
}