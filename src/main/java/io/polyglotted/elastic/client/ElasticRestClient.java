package io.polyglotted.elastic.client;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.model.MapResult.ImmutableResult;
import io.polyglotted.common.util.HttpRequestBuilder.HttpReqType;
import io.polyglotted.common.util.MapBuilder.ImmutableMapBuilder;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import org.apache.http.ConnectionClosedException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.StringEntity;
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.ConnectException;
import java.util.List;
import java.util.Map;

import static io.polyglotted.common.model.MapResult.immutableResult;
import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.common.util.BaseSerializer.deserializeToList;
import static io.polyglotted.common.util.HttpRequestBuilder.HttpReqType.DELETE;
import static io.polyglotted.common.util.HttpRequestBuilder.HttpReqType.GET;
import static io.polyglotted.common.util.HttpRequestBuilder.HttpReqType.POST;
import static io.polyglotted.common.util.HttpRequestBuilder.HttpReqType.PUT;
import static io.polyglotted.common.util.MapBuilder.immutableMapBuilder;
import static io.polyglotted.common.util.MapRetriever.asMap;
import static io.polyglotted.common.util.MapRetriever.mapVal;
import static io.polyglotted.common.util.MapRetriever.reqdStr;
import static io.polyglotted.common.util.StrUtil.notNullOrEmpty;
import static io.polyglotted.common.util.ThreadUtil.safeSleep;
import static io.polyglotted.elastic.client.ElasticException.checkState;
import static io.polyglotted.elastic.client.ElasticException.throwEx;
import static java.util.Collections.emptyMap;
import static org.apache.http.HttpStatus.SC_MULTIPLE_CHOICES;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE) @Accessors(fluent = true)
public class ElasticRestClient implements ElasticClient {
    private final RestHighLevelClient internalClient;
    @Nullable @Getter private final AuthHeader bootstrapAuth;

    ElasticRestClient(RestClientBuilder builder, AuthHeader bootstrapAuth) { this(new RestHighLevelClient(builder), bootstrapAuth); }

    @Override @SneakyThrows public void close() { internalClient.close(); }

    @SuppressWarnings("ALL")
    @Override public ElasticClient waitForStatus(AuthHeader auth, String status) {
        try {
            for (int i = 0; i <= 300; i++) {
                performCliRequest(auth, GET, "/_cluster/health?wait_for_status=" + status); break;
            }
        } catch (ConnectException | ConnectionClosedException retry) {
            safeSleep(1000); waitForStatus(auth, status);
        } catch (Exception ioe) { throw throwEx("waitForStatus failed", ioe); }
        return this;
    }

    @Override public MapResult clusterHealth(AuthHeader auth) { return deserialize(simpleGet(auth, "/_cluster/health", "clusterHealth")); }

    @Override public boolean indexExists(AuthHeader auth, String index) {
        try {
            return internalClient.getLowLevelClient().performRequest("HEAD", "/" + index, auth.headers())
                .getStatusLine().getStatusCode() == SC_OK;
        } catch (Exception ioe) { throw throwEx("indexExists failed", ioe); }
    }

    @Override public MapResult indexNameFor(AuthHeader auth, String alias) {
        ImmutableMapBuilder<String, String> result = immutableMapBuilder();
        try {
            List<Map<String, Object>> list = deserializeToList(performCliRequest(auth, POST, "/_cat/aliases"
                + (notNullOrEmpty(alias) ? "/" + alias : "") + "?h=index,alias&format=json"));
            for (Map<String, Object> map : list) { result.put(reqdStr(map, "alias"), reqdStr(map, "index")); }
            return result.result();
        } catch (Exception ioe) { throw throwEx("indexNameFor failed", ioe); }
    }

    @Override public String createIndex(AuthHeader auth, CreateIndexRequest request) {
        try {
            CreateIndexResponse response = internalClient.indices().create(request, auth.headers());
            checkState(response.isAcknowledged() && response.isShardsAcknowledged(), "unable to create index");
            return request.index();
        } catch (Exception e) { throw throwEx("createIndex failed", e); }
    }

    @Override public void dropIndex(AuthHeader auth, String index) {
        try {
            DeleteIndexResponse response = internalClient.indices().delete(new DeleteIndexRequest(index), auth.headers());
            checkState(response.isAcknowledged(), "unable to drop index");
        } catch (Exception ioe) { throw throwEx("dropIndex failed", ioe); }
    }

    @Override public void forceRefresh(AuthHeader auth, String index) {
        try {
            performCliRequest(auth, POST, "/" + index + "/_refresh");
        } catch (Exception ioe) { throw throwEx("forceRefresh failed", ioe); }
    }

    @Override public String getSettings(AuthHeader auth, String index) { return simpleGet(auth, "/" + index + "/_settings", "getSettings"); }

    @Override public ImmutableResult getMapping(AuthHeader auth, String index) {
        try {
            MapResult result = deserialize(performCliRequest(auth, GET, "/" + index + "/_mapping/_doc"));
            return immutableResult(mapVal(asMap(result.first()), "mappings"));
        } catch (Exception e) { throw throwEx("getMapping failed", e); }
    }

    @Override public void putPipeline(AuthHeader auth, String id, String body) { simplePut(auth, PUT, "/_ingest/pipeline/" + id, body, "putPipeline"); }

    @Override public boolean pipelineExists(AuthHeader auth, String id) { return simpleGet(auth, "/_ingest/pipeline/" + id, "pipelineExists") != null; }

    @Override public void deletePipeline(AuthHeader auth, String id) { simpleDelete(auth, "/_ingest/pipeline/" + id, "deletePipeline"); }

    @Override public IndexResponse index(AuthHeader auth, IndexRequest request) {
        try { return internalClient.index(request, auth.headers()); } catch (IOException ioe) { throw throwEx("index failed", ioe); }
    }

    @Override public DeleteResponse delete(AuthHeader auth, DeleteRequest request) {
        try { return internalClient.delete(request, auth.headers()); } catch (IOException ioe) { throw throwEx("delete failed", ioe); }
    }

    @Override public BulkResponse bulk(AuthHeader auth, BulkRequest request) {
        try { return internalClient.bulk(request, auth.headers()); } catch (IOException ioe) { throw throwEx("bulk failed", ioe); }
    }

    @Override public void bulkAsync(AuthHeader auth, BulkRequest request, ActionListener<BulkResponse> listener) {
        try { internalClient.bulkAsync(request, listener, auth.headers()); } catch (Exception ioe) { throw throwEx("bulkAsync failed", ioe); }
    }

    @Override public boolean exists(AuthHeader auth, GetRequest request) {
        try { return internalClient.exists(request, auth.headers()); } catch (IOException ioe) { throw throwEx("exists failed", ioe); }
    }

    @Override public MultiGetResponse multiGet(AuthHeader auth, MultiGetRequest request) {
        try { return internalClient.multiGet(request, auth.headers()); } catch (IOException ioe) { throw throwEx("multiGet failed", ioe); }
    }

    @Override public SearchResponse search(AuthHeader auth, SearchRequest request) {
        try { return internalClient.search(request, auth.headers()); } catch (IOException ioe) { throw throwEx("search failed", ioe); }
    }

    @Override public SearchResponse searchScroll(AuthHeader auth, SearchScrollRequest request) {
        try { return internalClient.searchScroll(request, auth.headers()); } catch (IOException ioe) { throw throwEx("searchScroll failed", ioe); }
    }

    @Override public ClearScrollResponse clearScroll(AuthHeader auth, ClearScrollRequest request) {
        try { return internalClient.clearScroll(request, auth.headers()); } catch (IOException ioe) { throw throwEx("clearScroll failed", ioe); }
    }

    @Override public MapResult xpackPut(AuthHeader auth, XPackApi api, String id, String body) {
        String putNotFound = simplePut(auth, api.type, api.endpoint + id, body, api.name().toLowerCase() + "Put");
        return putNotFound == null ? immutableResult() : deserialize(putNotFound);
    }

    @Override public MapResult xpackGet(AuthHeader auth, XPackApi api, String id) {
        String getNotFound = simpleGet(auth, api.endpoint + id, api.name().toLowerCase() + "Get");
        return getNotFound == null ? immutableResult() : deserialize(getNotFound);
    }

    @Override public void xpackDelete(AuthHeader auth, XPackApi api, String id) {
        simpleDelete(auth, api.endpoint + id, api.name().toLowerCase() + "Delete");
    }

    @Override public void xpackDelete(AuthHeader auth, XPackApi api, String id, String body) {
        try {
            performCliRequest(DELETE, api.endpoint + id, emptyMap(), new StringEntity(body, APPLICATION_JSON), auth.headers());
        } catch (Exception ioe) { throw throwEx(api.name().toLowerCase() + "Delete failed", ioe); }
    }

    private String simpleGet(AuthHeader auth, String endpoint, String methodName) {
        Exception throwable;
        try {
            return performCliRequest(auth, GET, endpoint);

        } catch (ResponseException re) {
            if (re.getResponse().getStatusLine().getStatusCode() == 404) { return null; }
            throwable = re;
        } catch (Exception ioe) { throwable = ioe; }
        throw throwEx(methodName + " failed", throwable);
    }

    private String simplePut(AuthHeader auth, HttpReqType reqType, String endpoint, String body, String methodName) {
        try {
            return performCliRequest(reqType, endpoint, emptyMap(), new StringEntity(body, APPLICATION_JSON), auth.headers());
        } catch (Exception ioe) { throw throwEx(methodName + " failed", ioe); }
    }

    private void simpleDelete(AuthHeader auth, String endpoint, String methodName) {
        try {
            performCliRequest(auth, DELETE, endpoint);
        } catch (Exception ioe) { throw throwEx(methodName + " failed", ioe); }
    }

    private String performCliRequest(AuthHeader auth, HttpReqType method, String endpoint) throws IOException {
        return performCliRequest(method, endpoint, emptyMap(), null, auth.headers());
    }

    private String performCliRequest(HttpReqType method, String endpoint, Map<String, String> params,
                                     HttpEntity entity, Header... headers) throws IOException {
        Response response = internalClient.getLowLevelClient().performRequest(method.name(), endpoint, params, entity, headers);
        int statusCode = response.getStatusLine().getStatusCode();
        checkState(statusCode >= SC_OK && statusCode < SC_MULTIPLE_CHOICES, response.getStatusLine().getReasonPhrase());
        return EntityUtils.toString(response.getEntity());
    }
}