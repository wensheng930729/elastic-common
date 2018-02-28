package io.polyglotted.elastic.common;

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

import java.util.Map;
import java.util.Set;

@SuppressWarnings("unused")
public interface ElasticClient extends AutoCloseable {

    boolean indexExists(String index);

    Set<String> getIndices(String alias);

    String getIndexMeta(String... indices);

    String getSettings(String... indices);

    String getMapping(String index, String type);

    String createIndex(CreateIndexRequest request);

    void forceRefresh(String... indices);

    String dropIndex(String... indices);

    void waitForStatus(String status);

    Map<String, Object> clusterHealth();

    void buildPipeline(String id, String resource);

    boolean pipelineExists(String id);

    IndexResponse index(IndexRequest request);

    UpdateResponse update(UpdateRequest request);

    DeleteResponse delete(DeleteRequest request);

    BulkResponse bulk(BulkRequest request);

    void bulkAsync(BulkRequest bulkRequest, ActionListener<BulkResponse> listener);

    GetResponse get(GetRequest request);

    MultiGetResponse multiGet(MultiGetRequest request);

    SearchResponse search(SearchRequest request);

    SearchResponse searchScroll(SearchScrollRequest request);

    ClearScrollResponse clearScroll(ClearScrollRequest request);
}