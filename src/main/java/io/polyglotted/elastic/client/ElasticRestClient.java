package io.polyglotted.elastic.client;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.model.MapResult.ImmutableResult;
import io.polyglotted.common.util.HttpRequestBuilder.HttpReqType;
import io.polyglotted.elastic.common.EsAuth;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.http.ConnectionClosedException;
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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Map;

import static io.polyglotted.common.model.MapResult.immutableResult;
import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.common.util.HttpRequestBuilder.HttpReqType.DELETE;
import static io.polyglotted.common.util.HttpRequestBuilder.HttpReqType.GET;
import static io.polyglotted.common.util.HttpRequestBuilder.HttpReqType.POST;
import static io.polyglotted.common.util.HttpRequestBuilder.HttpReqType.PUT;
import static io.polyglotted.common.util.MapRetriever.asMap;
import static io.polyglotted.common.util.MapRetriever.mapVal;
import static io.polyglotted.common.util.ThreadUtil.safeSleep;
import static io.polyglotted.elastic.client.ElasticException.checkState;
import static io.polyglotted.elastic.client.ElasticException.throwEx;
import static java.util.Collections.emptyMap;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.HttpStatus.SC_MULTIPLE_CHOICES;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class ElasticRestClient implements ElasticClient {
    private final RestHighLevelClient internalClient;

    ElasticRestClient(RestClientBuilder builder) { this(new RestHighLevelClient(builder)); }

    @Override @SneakyThrows public void close() { internalClient.close(); }

    @SuppressWarnings("ALL")
    @Override public ElasticClient waitForStatus(EsAuth auth, String status) {
        try {
            for (int i = 0; i <= 300; i++) {
                performCliRequest(auth, GET, "/_cluster/health?wait_for_status=" + status); break;
            }
        } catch (ConnectException | ConnectionClosedException retry) {
            safeSleep(1000); waitForStatus(auth, status);
        } catch (Exception ioe) { throw throwEx("waitForStatus failed", ioe); }
        return this;
    }

    @Override public MapResult clusterHealth(EsAuth auth) {
        try {
            return deserialize(performCliRequest(auth, GET, "/_cluster/health"));
        } catch (Exception ioe) { throw throwEx("clusterHealth failed", ioe); }
    }

    @Override public boolean indexExists(EsAuth auth, String index) {
        try {
            return internalClient.getLowLevelClient().performRequest("HEAD", "/" + index, auth.header())
                .getStatusLine().getStatusCode() == SC_OK;
        } catch (Exception ioe) { throw throwEx("indexExists failed", ioe); }
    }

    @Override public String createIndex(EsAuth auth, CreateIndexRequest request) {
        try {
            CreateIndexResponse response = internalClient.indices().create(request, auth.header());
            checkState(response.isAcknowledged() && response.isShardsAcknowledged(), "unable to create index");
            return request.index();
        } catch (Exception e) { throw throwEx("createIndex failed", e); }
    }

    @Override public void dropIndex(EsAuth auth, String index) {
        try {
            DeleteIndexResponse response = internalClient.indices().delete(new DeleteIndexRequest(index), auth.header());
            checkState(response.isAcknowledged(), "unable to drop index");
        } catch (Exception ioe) { throw throwEx("dropIndex failed", ioe); }
    }

    @Override public void forceRefresh(EsAuth auth, String index) {
        try {
            performCliRequest(auth, POST, "/" + index + "/_refresh");
        } catch (Exception ioe) { throw throwEx("forceRefresh failed", ioe); }
    }

    @Override public String getSettings(EsAuth auth, String index) {
        try {
            return performCliRequest(auth, GET, "/" + index + "/_settings");
        } catch (Exception e) { throw throwEx("getSettings failed", e); }
    }

    @Override public ImmutableResult getMapping(EsAuth auth, String index) {
        try {
            MapResult result = deserialize(performCliRequest(auth, GET, "/" + index + "/_mapping/_doc"));
            return immutableResult(mapVal(asMap(result.first()), "mappings"));
        } catch (Exception e) { throw throwEx("getMapping failed", e); }
    }

    @Override public void buildPipeline(EsAuth auth, String id, String resource) {
        try {
            performCliRequest(PUT, "/_ingest/pipeline/" + id, emptyMap(), new StringEntity(resource), auth.header(), ctypeHeader());
        } catch (Exception ioe) { throw throwEx("buildPipeline failed", ioe); }
    }

    @Override public boolean pipelineExists(EsAuth auth, String id) {
        Exception throwable;
        try {
            performCliRequest(auth, GET, "/_ingest/pipeline/" + id); return true;

        } catch (ResponseException re) {
            if (re.getResponse().getStatusLine().getStatusCode() == 404) { return false; }
            throwable = re;

        } catch (Exception ioe) { throwable = ioe; }
        throw throwEx("pipelineExists failed", throwable);
    }

    @Override public void deletePipeline(EsAuth auth, String id) {
        try {
            performCliRequest(auth, DELETE, "/_ingest/pipeline/" + id);
        } catch (Exception ioe) { throw throwEx("deletePipeline failed", ioe); }
    }

    @Override public IndexResponse index(EsAuth auth, IndexRequest request) {
        try { return internalClient.index(request, auth.header()); } catch (IOException ioe) { throw throwEx("index failed", ioe); }
    }

    @Override public DeleteResponse delete(EsAuth auth, DeleteRequest request) {
        try { return internalClient.delete(request, auth.header()); } catch (IOException ioe) { throw throwEx("delete failed", ioe); }
    }

    @Override public BulkResponse bulk(EsAuth auth, BulkRequest request) {
        try { return internalClient.bulk(request, auth.header()); } catch (IOException ioe) { throw throwEx("bulk failed", ioe); }
    }

    @Override public void bulkAsync(EsAuth auth, BulkRequest request, ActionListener<BulkResponse> listener) {
        try { internalClient.bulkAsync(request, listener, auth.header()); } catch (Exception ioe) { throw throwEx("bulkAsync failed", ioe); }
    }

    @Override public boolean exists(EsAuth auth, GetRequest request) {
        try { return internalClient.exists(request, auth.header()); } catch (IOException ioe) { throw throwEx("exists failed", ioe); }
    }

    @Override public GetResponse get(EsAuth auth, GetRequest request) {
        try { return internalClient.get(request, auth.header()); } catch (IOException ioe) { throw throwEx("get failed", ioe); }
    }

    @Override public MultiGetResponse multiGet(EsAuth auth, MultiGetRequest request) {
        try { return internalClient.multiGet(request, auth.header()); } catch (IOException ioe) { throw throwEx("multiGet failed", ioe); }
    }

    @Override public SearchResponse search(EsAuth auth, SearchRequest request) {
        try { return internalClient.search(request, auth.header()); } catch (IOException ioe) { throw throwEx("search failed", ioe); }
    }

    @Override public SearchResponse searchScroll(EsAuth auth, SearchScrollRequest request) {
        try { return internalClient.searchScroll(request, auth.header()); } catch (IOException ioe) { throw throwEx("searchScroll failed", ioe); }
    }

    @Override public ClearScrollResponse clearScroll(EsAuth auth, ClearScrollRequest request) {
        try { return internalClient.clearScroll(request, auth.header()); } catch (IOException ioe) { throw throwEx("clearScroll failed", ioe); }
    }

    private String performCliRequest(EsAuth auth, HttpReqType method, String endpoint) throws IOException {
        return performCliRequest(method, endpoint, emptyMap(), null, auth.header());
    }

    private String performCliRequest(HttpReqType method, String endpoint, Map<String, String> params,
                                     HttpEntity entity, Header... headers) throws IOException {
        Response response = internalClient.getLowLevelClient().performRequest(method.name(), endpoint, params, entity, headers);
        int statusCode = response.getStatusLine().getStatusCode();
        checkState(statusCode >= SC_OK && statusCode < SC_MULTIPLE_CHOICES, response.getStatusLine().getReasonPhrase());
        return EntityUtils.toString(response.getEntity());
    }

    private static Header ctypeHeader() { return new BasicHeader(CONTENT_TYPE, APPLICATION_JSON.getMimeType()); }
}