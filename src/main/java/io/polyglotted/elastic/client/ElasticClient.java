package io.polyglotted.elastic.client;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.common.EsAuth;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

public interface ElasticClient extends AutoCloseable {

    void waitForStatus(EsAuth auth, String status);

    MapResult clusterHealth(EsAuth auth);

    boolean indexExists(EsAuth auth, String index);

    void createIndex(EsAuth auth, CreateIndexRequest request);

    void dropIndex(EsAuth auth, String index);

    void forceRefresh(EsAuth auth, String index);

    String getSettings(EsAuth auth, String index);

    String getMapping(EsAuth auth, String index);

    void buildPipeline(EsAuth auth, String id, String resource);

    boolean pipelineExists(EsAuth auth, String id);

    void deletePipeline(EsAuth auth, String id);

    IndexResponse index(EsAuth auth, IndexRequest request);

    UpdateResponse update(EsAuth auth, UpdateRequest request);

    DeleteResponse delete(EsAuth auth, DeleteRequest request);

    BulkResponse bulk(EsAuth auth, BulkRequest bulk);

    void bulkAsync(EsAuth auth, BulkRequest bulkRequest, ActionListener<BulkResponse> listener);

    boolean exists(EsAuth auth, GetRequest request);

    GetResponse get(EsAuth auth, GetRequest request);

    MultiGetResponse multiGet(EsAuth auth, MultiGetRequest request);

    SearchResponse search(EsAuth auth, SearchRequest request);

    SearchResponse searchScroll(EsAuth auth, SearchScrollRequest request);

    ClearScrollResponse clearScroll(EsAuth auth, ClearScrollRequest request);
}