package io.polyglotted.elastic.client;

import io.polyglotted.common.model.MapResult;
import org.apache.http.Header;
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

import static io.polyglotted.elastic.client.EsRestClient.authHeader;
import static io.polyglotted.elastic.client.EsRestClient.ctypeHeader;

public interface ElasticClient extends AutoCloseable {

    void waitForStatus(String status, Header... headers);

    default void waitForStatus(String status, String auth) { waitForStatus(status, authHeader(auth)); }

    MapResult clusterHealth(Header... headers);

    default MapResult clusterHealth(String auth) { return clusterHealth(authHeader(auth)); }

    default boolean indexExists(String index, String auth) { return indexExists(index, authHeader(auth)); }

    boolean indexExists(String index, Header... headers);

    default void createIndex(CreateIndexRequest request, String auth) { createIndex(request, authHeader(auth)); }

    void createIndex(CreateIndexRequest request, Header... headers);

    default void dropIndex(String index, String auth) { dropIndex(index, authHeader(auth)); }

    void dropIndex(String index, Header... headers);

    default void forceRefresh(String index, String auth) { forceRefresh(index, authHeader(auth)); }

    void forceRefresh(String index, Header... headers);

    default String getSettings(String index, String auth) { return getSettings(index, authHeader(auth)); }

    String getSettings(String index, Header... headers);

    default String getMapping(String index, String auth) { return getMapping(index, authHeader(auth)); }

    String getMapping(String index, Header... headers);

    default void buildPipeline(String id, String resource, String auth) { buildPipeline(id, resource, authHeader(auth), ctypeHeader()); }

    void buildPipeline(String id, String resource, Header... headers);

    default boolean pipelineExists(String id, String auth) { return pipelineExists(id, authHeader(auth)); }

    boolean pipelineExists(String id, Header... headers);

    default void deletePipeline(String id, String auth) { deletePipeline(id, authHeader(auth)); }

    void deletePipeline(String id, Header... headers);

    default IndexResponse index(IndexRequest request, String auth) { return index(request, authHeader(auth)); }

    IndexResponse index(IndexRequest request, Header... headers);

    default UpdateResponse update(UpdateRequest request, String auth) { return update(request, authHeader(auth)); }

    UpdateResponse update(UpdateRequest request, Header... headers);

    default DeleteResponse delete(DeleteRequest request, String auth) { return delete(request, authHeader(auth)); }

    DeleteResponse delete(DeleteRequest request, Header... headers);

    default BulkResponse bulk(BulkRequest bulk, String auth) { return bulk(bulk, authHeader(auth)); }

    BulkResponse bulk(BulkRequest bulk, Header... headers);

    default void bulkAsync(BulkRequest bulk, ActionListener<BulkResponse> listener, String auth) { bulkAsync(bulk, listener, authHeader(auth)); }

    void bulkAsync(BulkRequest bulkRequest, ActionListener<BulkResponse> listener, Header... headers);

    default boolean exists(GetRequest request, String auth) { return exists(request, authHeader(auth)); }

    boolean exists(GetRequest request, Header... headers);

    default GetResponse get(GetRequest request, String auth) { return get(request, authHeader(auth)); }

    GetResponse get(GetRequest request, Header... headers);

    default MultiGetResponse multiGet(MultiGetRequest request, String auth) { return multiGet(request, authHeader(auth)); }

    MultiGetResponse multiGet(MultiGetRequest request, Header... headers);

    default SearchResponse search(SearchRequest request, String auth) { return search(request, authHeader(auth)); }

    SearchResponse search(SearchRequest request, Header... headers);

    default SearchResponse searchScroll(SearchScrollRequest request, String auth) { return searchScroll(request, authHeader(auth)); }

    SearchResponse searchScroll(SearchScrollRequest request, Header... headers);

    default ClearScrollResponse clearScroll(ClearScrollRequest request, String auth) { return clearScroll(request, authHeader(auth)); }

    ClearScrollResponse clearScroll(ClearScrollRequest request, Header... headers);
}