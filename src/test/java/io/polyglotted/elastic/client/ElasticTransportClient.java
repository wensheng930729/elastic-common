package io.polyglotted.elastic.client;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.model.MapResult.ImmutableResult;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
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
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.Iterator;
import java.util.Locale;

import static io.polyglotted.common.model.MapResult.immutableResult;
import static io.polyglotted.common.util.Assertions.checkBool;
import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.common.util.MapBuilder.immutableMap;
import static io.polyglotted.common.util.MapBuilder.immutableMapBuilder;
import static io.polyglotted.elastic.client.ElasticException.checkState;
import static io.polyglotted.elastic.client.ElasticException.throwEx;
import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

@RequiredArgsConstructor @Accessors(fluent = true)
public class ElasticTransportClient implements ElasticClient {
    private final Client internalClient;
    @NonNull @Getter private final AuthHeader bootstrapAuth;

    @Override public void close() { internalClient.close(); }

    @Override public ElasticClient waitForStatus(AuthHeader auth, String status) {
        try {
            ClusterHealthResponse clusterHealth = client(auth).admin().cluster().prepareHealth().setWaitForNoRelocatingShards(true)
                .setWaitForStatus(ClusterHealthStatus.fromString(status)).execute().actionGet();
            checkBool(clusterHealth.getStatus() != ClusterHealthStatus.RED, "cluster has errors");
        } catch (Exception ex) { throw throwEx("waitForStatus failed", ex); }
        return this;
    }

    @Override public MapResult clusterHealth(AuthHeader auth) {
        try {
            ClusterHealthResponse health = internalClient.admin().cluster().health(new ClusterHealthRequest()).actionGet();
            return immutableMapBuilder()
                .put("cluster_name", health.getClusterName())
                .put("status", health.getStatus().name().toLowerCase(Locale.ROOT))
                .put("timed_out", health.isTimedOut())
                .put("number_of_nodes", health.getNumberOfNodes())
                .put("number_of_data_nodes", health.getNumberOfDataNodes())
                .put("active_primary_shards", health.getActivePrimaryShards())
                .put("active_shards", health.getActiveShards())
                .put("relocating_shards", health.getRelocatingShards())
                .put("initializing_shards", health.getInitializingShards())
                .put("unassigned_shards", health.getUnassignedShards())
                .put("delayed_unassigned_shards", health.getDelayedUnassignedShards())
                .put("number_of_pending_tasks", health.getNumberOfPendingTasks())
                .put("number_of_in_flight_fetch", health.getNumberOfInFlightFetch())
                .put("task_max_waiting_in_queue_millis", health.getTaskMaxWaitingTime().millis() == 0 ? "-" : health.getTaskMaxWaitingTime().getStringRep())
                .put("active_shards_percent_as_number", String.format(Locale.ROOT, "%1.1f%%", health.getActiveShardsPercent()))
                .result();
        } catch (Exception ex) { throw throwEx("clusterHealth failed", ex); }
    }

    @Override public boolean indexExists(AuthHeader auth, String repo) {
        try {
            return client(auth).admin().indices().exists(new IndicesExistsRequest(repo)).actionGet().isExists();
        } catch (Exception ex) { throw throwEx("indexExists failed", ex); }
    }

    @Override public MapResult indexNameFor(AuthHeader auth, String alias) {
        AliasOrIndex aliasOrIndex = getMeta(auth, alias).getAliasAndIndexLookup().get(alias);
        return (aliasOrIndex != null) ? immutableResult(alias, aliasOrIndex.getIndices().get(0).getIndex().getName()) : immutableResult();
    }

    @Override public String createIndex(AuthHeader auth, CreateIndexRequest request) {
        try {
            CreateIndexResponse response = client(auth).admin().indices().create(request).actionGet();
            checkState(response.isAcknowledged() && response.isShardsAcknowledged(), "unable to create index");
            return request.index();
        } catch (Exception ex) { throw throwEx("createIndex failed", ex); }
    }

    @Override public void dropIndex(AuthHeader auth, String index) {
        try {
            DeleteIndexResponse response = client(auth).admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
            checkState(response.isAcknowledged(), "unable to drop index");
        } catch (Exception ioe) { throw throwEx("dropIndex failed", ioe); }
    }

    @Override public void forceRefresh(AuthHeader auth, String repo) {
        try {
            client(auth).admin().indices().refresh(refreshRequest(repo)).actionGet();
        } catch (Exception ex) { throw throwEx("refresh failed", ex); }
    }

    @Override @SneakyThrows public String getSettings(AuthHeader auth, String repo) {
        XContentBuilder builder = jsonBuilder().startObject();
        MetaData indexMetaDatas = getMeta(auth, repo);
        ImmutableOpenMap<String, IndexMetaData> getIndices = indexMetaDatas.getIndices();
        Iterator<String> indexIt = getIndices.keysIt();
        while (indexIt.hasNext()) {
            String index = indexIt.next();
            IndexMetaData metaData = getIndices.get(index);
            builder.startObject(index).startObject("settings");
            Settings settings = metaData.getSettings();
            settings.toXContent(builder, EMPTY_PARAMS);
            builder.endObject().endObject();
        }
        builder.endObject();
        return builder.string();
    }

    private MetaData getMeta(AuthHeader auth, String repo) {
        try {
            return client(auth).admin().cluster().prepareState().setIndices(repo).execute().actionGet().getState().metaData();
        } catch (Exception ex) { throw throwEx("getMeta failed", ex); }
    }

    @Override public MapResult putMapping(AuthHeader auth, String index, String mappingJson) {
        try {
            PutMappingRequest request = putMappingRequest(index).source(mappingJson, JSON).updateAllTypes(true);
            PutMappingResponse response = client(auth).admin().indices().putMapping(request).actionGet();
            return immutableResult("acknowledged", response.isAcknowledged());
        } catch (Exception ex) { throw throwEx("putMapping failed", ex); }
    }

    @Override @SneakyThrows public ImmutableResult getMapping(AuthHeader auth, String repo) {
        ImmutableOpenMap<String, IndexMetaData> getIndices = getMeta(auth, repo).getIndices();
        Iterator<String> indexIt = getIndices.keysIt();
        while (indexIt.hasNext()) {
            ImmutableOpenMap<String, MappingMetaData> mappings = getIndices.get(indexIt.next()).getMappings();
            Iterator<String> mIt = mappings.keysIt();
            if (mIt.hasNext()) { return immutableResult(deserialize(mappings.get(mIt.next()).source().string())); }
        }
        return immutableResult();
    }

    @Override public void putPipeline(AuthHeader auth, String id, String resource) {
        try {
            checkState(client(auth).admin().cluster().preparePutPipeline(id, new BytesArray(resource), JSON)
                .execute().actionGet().isAcknowledged(), "unable to putPipeline");
        } catch (Exception ex) { throw throwEx("putPipeline failed", ex); }
    }

    @Override public boolean pipelineExists(AuthHeader auth, String id) {
        try {
            return client(auth).admin().cluster().prepareGetPipeline(id).execute().actionGet().isFound();
        } catch (Exception ex) { throw throwEx("pipelineExists failed", ex); }
    }

    @Override public void deletePipeline(AuthHeader auth, String id) {
        try {
            checkState(client(auth).admin().cluster().prepareDeletePipeline(id)
                .execute().actionGet().isAcknowledged(), "unable to deletePipeline");
        } catch (Exception ex) { throw throwEx("deletePipeline failed", ex); }
    }

    @Override public void putTemplate(AuthHeader auth, String name, String body) {
        try {
            checkState(client(auth).admin().indices().preparePutTemplate(name).setSource(new BytesArray(body), JSON)
                .execute().actionGet().isAcknowledged(), "unable to putTemplate");
        } catch (Exception ex) { throw throwEx("putPipeline failed", ex); }
    }

    @Override public boolean templateExists(AuthHeader auth, String name) {
        try {
            return !client(auth).admin().indices().prepareGetTemplates(name).execute().actionGet().getIndexTemplates().isEmpty();
        } catch (Exception ex) { throw throwEx("templateExists failed", ex); }
    }

    @Override public void deleteTemplate(AuthHeader auth, String name) {
        try {
            checkState(client(auth).admin().indices().prepareDeleteTemplate(name)
                .execute().actionGet().isAcknowledged(), "unable to deleteTemplate");
        } catch (Exception ex) { throw throwEx("putPipeline failed", ex); }
    }

    @Override public IndexResponse index(AuthHeader auth, IndexRequest request) {
        try { return client(auth).index(request).actionGet(); } catch (Exception ex) { throw throwEx("index failed", ex); }
    }

    @Override public DeleteResponse delete(AuthHeader auth, DeleteRequest request) {
        try { return client(auth).delete(request).actionGet(); } catch (Exception ex) { throw throwEx("delete failed", ex); }
    }

    @Override public BulkResponse bulk(AuthHeader auth, BulkRequest bulk) {
        try { return client(auth).bulk(bulk).actionGet(); } catch (Exception ex) { throw throwEx("bulk failed", ex); }
    }

    @Override public void bulkAsync(AuthHeader auth, BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
        try { client(auth).bulk(bulkRequest, listener); } catch (Exception ex) { throw throwEx("bulkAsync failed", ex); }
    }

    @Override public boolean exists(AuthHeader auth, GetRequest request) {
        try { return client(auth).get(request).actionGet().isExists(); } catch (Exception ex) { throw throwEx("exists failed", ex); }
    }

    @Override public MultiGetResponse multiGet(AuthHeader auth, MultiGetRequest request) {
        try { return client(auth).multiGet(request).actionGet(); } catch (Exception ex) { throw throwEx("multiGet failed", ex); }
    }

    @Override public SearchResponse search(AuthHeader auth, SearchRequest request) {
        try { return client(auth).search(request).actionGet(); } catch (Exception ex) { throw throwEx("search failed", ex); }
    }

    @Override public SearchResponse searchScroll(AuthHeader auth, SearchScrollRequest request) {
        try { return client(auth).searchScroll(request).actionGet(); } catch (Exception ex) { throw throwEx("searchScroll failed", ex); }
    }

    @Override public ClearScrollResponse clearScroll(AuthHeader auth, ClearScrollRequest request) {
        try { return client(auth).clearScroll(request).actionGet(); } catch (Exception ex) { throw throwEx("clearScroll failed", ex); }
    }

    @Override public MultiSearchResponse multiSearch(AuthHeader auth, MultiSearchRequest request) {
        try { return client(auth).multiSearch(request).actionGet(); } catch (Exception ex) { throw throwEx("multiSearch failed", ex); }
    }

    @Override public MapResult xpackPut(AuthHeader auth, XPackApi api, String id, String body) {
        return null;
    }

    @Override public MapResult xpackGet(AuthHeader auth, XPackApi api, String id) {
        return null;
    }

    @Override public void xpackDelete(AuthHeader auth, XPackApi api, String id) {
    }

    @Override public void xpackDelete(AuthHeader auth, XPackApi api, String id, String body) {
    }

    private Client client(AuthHeader auth) { return internalClient.filterWithHeader(immutableMap("Authorization", auth.authHeader)); }
}