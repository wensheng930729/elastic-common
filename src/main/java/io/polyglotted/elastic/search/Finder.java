package io.polyglotted.elastic.search;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.MapBuilder;
import io.polyglotted.common.util.MapBuilder.ImmutableMapBuilder;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.EsAuth;
import io.polyglotted.elastic.index.IndexRecord;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;

import java.util.List;
import java.util.Map;

import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.util.MapBuilder.immutableMap;
import static io.polyglotted.common.util.StrUtil.nullOrEmpty;
import static io.polyglotted.elastic.common.MetaFields.keyString;

@Slf4j @SuppressWarnings({"unused", "WeakerAccess"})
public abstract class Finder {

    public static Map<String, MapResult> findAll(ElasticClient client, EsAuth auth, List<IndexRecord.Builder> builders) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (IndexRecord.Builder rec : builders) {
            if (nullOrEmpty(rec.id)) continue;
            log.debug("multi get " + rec.index + " " + rec.id + " " + rec.parent);
            multiGetRequest.add(new MultiGetRequest.Item(rec.index, "_doc", rec.id).routing(rec.parent));
        }

        if (multiGetRequest.getItems().size() == 0) return immutableMap();
        ImmutableMapBuilder<String, MapResult> result = MapBuilder.immutableMapBuilder();
        MultiGetResponse multiGetItemResponses = client.multiGet(auth, multiGetRequest);
        for (MultiGetItemResponse item : multiGetItemResponses.getResponses()) {

            GetResponse get = checkMultiGet(item).getResponse();
            if (get.isExists() && !get.isSourceEmpty()) {
                MapResult document = simpleResult(get.getSourceAsMap());
                result.put(keyString(document), document);
            }
        }
        return result.build();
    }

    public static MapResult findBy(ElasticClient client, EsAuth auth, IndexRecord rec) {
        return findBy(client, auth, rec.index, rec.model, rec.id, rec.parent);
    }

    public static MapResult findBy(ElasticClient client, EsAuth auth, String repo, String model, String id) {
        return findBy(client, auth, repo, model, id, null);
    }

    @SneakyThrows public static MapResult findBy(ElasticClient client, EsAuth auth, String repo, String model, String id, String parent) {
        GetResponse response = client.get(auth, new GetRequest(repo, "_doc", id).routing(parent));
        return response.isExists() && !response.isSourceEmpty() ? simpleResult(response.getSource()) : null;
    }

    private static MultiGetItemResponse checkMultiGet(MultiGetItemResponse item) {
        if (item.isFailed()) { throw new IllegalStateException("error multi-get item " + item.getFailure().getMessage()); } return item;
    }
}