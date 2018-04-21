package io.polyglotted.elastic.search;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.MapBuilder;
import io.polyglotted.common.util.MapBuilder.ImmutableMapBuilder;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.DocResult;
import io.polyglotted.elastic.common.EsAuth;
import io.polyglotted.elastic.index.IndexRecord;
import io.polyglotted.elastic.search.Expressions.BoolBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.util.List;
import java.util.Map;

import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.common.util.MapBuilder.immutableMap;
import static io.polyglotted.common.util.StrUtil.nullOrEmpty;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.KEY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.PARENT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.keyString;
import static io.polyglotted.elastic.common.Verbose.NONE;
import static io.polyglotted.elastic.search.Expressions.bool;
import static io.polyglotted.elastic.search.Expressions.equalsTo;
import static io.polyglotted.elastic.search.QueryMaker.filterToRequest;
import static io.polyglotted.elastic.search.ResponseBuilder.DocResultBuilder;
import static io.polyglotted.elastic.search.SearchUtil.getReturnedHits;

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

    public static DocResult findByKey(ElasticClient client, EsAuth auth, String repo, String key) { return findByKey(client, auth, repo, key, null); }

    public static DocResult findByKey(ElasticClient client, EsAuth auth, String repo, String key, FetchSourceContext context) {
        return findBy(client, auth, repo, equalsTo(KEY_FIELD, key), context);
    }

    public static DocResult findById(ElasticClient client, EsAuth auth, String repo, String id) { return findById(client, auth, repo, id, null, null); }

    public static DocResult findById(ElasticClient client, EsAuth auth, String repo, String id, String parent, FetchSourceContext context) {
        BoolBuilder idBuilder = bool().liveIndex().must(equalsTo(ID_FIELD, id));
        if (parent != null) { idBuilder.filter(equalsTo(PARENT_FIELD, parent)); }
        return findBy(client, auth, repo, idBuilder.build(), context);
    }

    @SneakyThrows private static DocResult findBy(ElasticClient client, EsAuth auth, String repo, Expression expr, FetchSourceContext context) {
        SearchResponse response = client.search(auth, filterToRequest(repo, expr, context, immutableList(), 1));
        return getReturnedHits(response) > 0 ? DocResultBuilder.buildFrom(response, NONE).get(0) : null;
    }

    private static MultiGetItemResponse checkMultiGet(MultiGetItemResponse item) {
        if (item.isFailed()) { throw new IllegalStateException("error multi-get item " + item.getFailure().getMessage()); } return item;
    }
}