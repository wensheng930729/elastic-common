package io.polyglotted.elastic.common;

import io.polyglotted.common.model.MapResult;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.elastic.common.ElasticException.checkState;
import static io.polyglotted.elastic.common.ElasticException.throwEx;
import static java.util.Collections.emptyMap;
import static org.apache.http.HttpHeaders.AUTHORIZATION;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.HttpStatus.SC_MULTIPLE_CHOICES;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class EsRestClient implements ElasticClient {
    private final RestHighLevelClient internalClient;

    EsRestClient(RestClientBuilder builder) { this(new RestHighLevelClient(builder)); }

    @Override public void close() throws Exception { internalClient.close(); }

    @Override public boolean indexExists(String index, Header... headers) {
        try {
            return internalClient.getLowLevelClient().performRequest("HEAD", "/" + index, headers).getStatusLine().getStatusCode() == SC_OK;
        } catch (Exception ioe) { throw throwEx("indexExists failed", ioe); }
    }

    @Override public String getSettings(String index, Header... headers) {
        try {
            return performCliRequest("GET", "/" + index + "/_settings", headers);
        } catch (Exception e) { throw throwEx("getSettings failed", e); }
    }

    @Override public String getMapping(String index, Header... headers) {
        try {
            return performCliRequest("GET", "/" + index + "/_mapping/_doc", headers);
        } catch (Exception e) { throw throwEx("getMapping failed", e); }
    }

    @Override public void createIndex(CreateIndexRequest request, Header... headers) {
        try {
            CreateIndexResponse response = internalClient.indices().create(request, headers);
            checkState(response.isAcknowledged() && response.isShardsAcknowledged(), "unable to create index");
        } catch (Exception e) { throw throwEx("createIndex failed", e); }
    }

    @Override public void forceRefresh(String index, Header... headers) {
        try {
            performCliRequest("POST", "/" + index + "/_refresh", headers);
        } catch (Exception ioe) { throw throwEx("forceRefresh failed", ioe); }
    }

    @Override public void dropIndex(String index, Header... headers) {
        try {
            DeleteIndexResponse response = internalClient.indices().delete(new DeleteIndexRequest(index), headers);
            checkState(response.isAcknowledged(), "unable to drop index");
        } catch (Exception ioe) { throw throwEx("dropIndex failed", ioe); }
    }

    @Override public void waitForStatus(String status, Header... headers) {
        try {
            performCliRequest("GET", "/_cluster/health?timeout=60s&wait_for_status=" + status, headers);
        } catch (Exception ioe) { throw throwEx("waitForStatus failed", ioe); }
    }

    @Override public MapResult clusterHealth(Header... headers) {
        try {
            return deserialize(performCliRequest("GET", "/_cluster/health", headers));
        } catch (Exception ioe) { throw throwEx("clusterHealth failed", ioe); }
    }

    @Override public void buildPipeline(String id, String resource, Header... headers) {
        try {
            performCliRequest("PUT", "_ingest/pipeline/" + id, new StringEntity(resource), headers);
        } catch (Exception ioe) { throw throwEx("buildPipeline failed", ioe); }
    }

    @Override public boolean pipelineExists(String id, Header... headers) {
        Exception throwable;
        try {
            performCliRequest("GET", "_ingest/pipeline/" + id, headers); return true;

        } catch (ResponseException re) {
            if (re.getResponse().getStatusLine().getStatusCode() == 404) { return false; }
            throwable = re;

        } catch (Exception ioe) { throwable = ioe; }
        throw throwEx("pipelineExists failed", throwable);
    }

    @Override public void deletePipeline(String id, Header... headers) {
        try {
            performCliRequest("DELETE", "_ingest/pipeline/" + id, headers);
        } catch (Exception ioe) { throw throwEx("deletePipeline failed", ioe); }
    }

    @Override public IndexResponse index(IndexRequest request, Header... headers) {
        try { return internalClient.index(request, headers); } catch (IOException ioe) { throw throwEx("index failed", ioe); }
    }

    @Override public UpdateResponse update(UpdateRequest request, Header... headers) {
        try { return internalClient.update(request, headers); } catch (IOException ioe) { throw throwEx("update failed", ioe); }
    }

    @Override public DeleteResponse delete(DeleteRequest request, Header... headers) {
        try { return internalClient.delete(request, headers); } catch (IOException ioe) { throw throwEx("delete failed", ioe); }
    }

    @Override public BulkResponse bulk(BulkRequest request, Header... headers) {
        try { return internalClient.bulk(request, headers); } catch (IOException ioe) { throw throwEx("bulk failed", ioe); }
    }

    @Override public void bulkAsync(BulkRequest request, ActionListener<BulkResponse> listener, Header... headers) {
        try { internalClient.bulkAsync(request, listener, headers); } catch (Exception ioe) { throw throwEx("bulkAsync failed", ioe); }
    }

    @Override public boolean exists(GetRequest request, Header... headers) {
        try { return internalClient.exists(request, headers); } catch (IOException ioe) { throw throwEx("exists failed", ioe); }
    }

    @Override public GetResponse get(GetRequest request, Header... headers) {
        try { return internalClient.get(request, headers); } catch (IOException ioe) { throw throwEx("get failed", ioe); }
    }

    @Override public MultiGetResponse multiGet(MultiGetRequest request, Header... headers) {
        try { return internalClient.multiGet(request, headers); } catch (IOException ioe) { throw throwEx("multiGet failed", ioe); }
    }

    @Override public SearchResponse search(SearchRequest request, Header... headers) {
        try { return internalClient.search(request, headers); } catch (IOException ioe) { throw throwEx("search failed", ioe); }
    }

    @Override public SearchResponse searchScroll(SearchScrollRequest request, Header... headers) {
        try { return internalClient.searchScroll(request, headers); } catch (IOException ioe) { throw throwEx("searchScroll failed", ioe); }
    }

    @Override public ClearScrollResponse clearScroll(ClearScrollRequest request, Header... headers) {
        try { return internalClient.clearScroll(request, headers); } catch (IOException ioe) { throw throwEx("clearScroll failed", ioe); }
    }

    private String performCliRequest(String method, String endpoint, Header... headers) throws IOException {
        return performCliRequest(method, endpoint, null, headers);
    }

    private String performCliRequest(String method, String endpoint, HttpEntity entity, Header... headers) throws IOException {
        Response response = internalClient.getLowLevelClient().performRequest(method, endpoint, emptyMap(), entity, headers);
        int statusCode = response.getStatusLine().getStatusCode();
        checkState(statusCode >= SC_OK && statusCode < SC_MULTIPLE_CHOICES, response.getStatusLine().getReasonPhrase());
        return EntityUtils.toString(response.getEntity());
    }

    static Header authHeader(String authorization) { return new BasicHeader(AUTHORIZATION, authorization); }
    static Header ctypeHeader() { return new BasicHeader(CONTENT_TYPE, APPLICATION_JSON.getMimeType()); }
}