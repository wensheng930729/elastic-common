package io.polyglotted.elastic.client;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.model.MapResult.ImmutableResult;
import io.polyglotted.elastic.common.EsAuth;
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

import java.io.Closeable;

public interface ElasticClient extends Closeable {
    void close();

    ElasticClient waitForStatus(EsAuth auth, String status);

    MapResult clusterHealth(EsAuth auth);

    boolean indexExists(EsAuth auth, String index);

    String createIndex(EsAuth auth, CreateIndexRequest request);

    void dropIndex(EsAuth auth, String index);

    void forceRefresh(EsAuth auth, String index);

    String getSettings(EsAuth auth, String index);

    ImmutableResult getMapping(EsAuth auth, String index);

    void putPipeline(EsAuth auth, String id, String resource);

    boolean pipelineExists(EsAuth auth, String id);

    void deletePipeline(EsAuth auth, String id);

    IndexResponse index(EsAuth auth, IndexRequest request);

    DeleteResponse delete(EsAuth auth, DeleteRequest request);

    BulkResponse bulk(EsAuth auth, BulkRequest bulk);

    void bulkAsync(EsAuth auth, BulkRequest bulkRequest, ActionListener<BulkResponse> listener);

    boolean exists(EsAuth auth, GetRequest request);

    MultiGetResponse multiGet(EsAuth auth, MultiGetRequest request);

    SearchResponse search(EsAuth auth, SearchRequest request);

    SearchResponse searchScroll(EsAuth auth, SearchScrollRequest request);

    ClearScrollResponse clearScroll(EsAuth auth, ClearScrollRequest request);

    MapResult xpackPut(EsAuth auth, XPackApi api, String id, String body);

    MapResult xpackGet(EsAuth auth, XPackApi api, String id);

    void xpackDelete(EsAuth auth, XPackApi api, String id);

    void xpackDelete(EsAuth auth, XPackApi api, String id, String body);
}