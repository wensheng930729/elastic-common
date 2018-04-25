package io.polyglotted.elastic.search;

import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.DocResult;
import io.polyglotted.elastic.common.EsAuth;
import io.polyglotted.elastic.index.BulkRecord;
import io.polyglotted.elastic.index.IndexRecord;
import io.polyglotted.elastic.search.Expressions.BoolBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.util.Map;

import static io.polyglotted.common.util.CollUtil.transform;
import static io.polyglotted.common.util.CollUtil.uniqueIndex;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.KEY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.MODEL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.PARENT_FIELD;
import static io.polyglotted.elastic.common.Verbose.NONE;
import static io.polyglotted.elastic.search.Expressions.bool;
import static io.polyglotted.elastic.search.Expressions.equalsTo;
import static io.polyglotted.elastic.search.Expressions.in;
import static io.polyglotted.elastic.search.QueryMaker.filterToRequest;
import static io.polyglotted.elastic.search.ResponseBuilder.DocResultBuilder;
import static io.polyglotted.elastic.search.SearchUtil.getReturnedHits;
import static org.elasticsearch.search.fetch.subphase.FetchSourceContext.FETCH_SOURCE;

@Slf4j @SuppressWarnings({"unused", "WeakerAccess"})
public abstract class Finder {

    public static Map<String, DocResult> findAll(ElasticClient client, EsAuth auth, BulkRecord record) {
        BoolBuilder idBuilder = idBuilder(record.model, in(ID_FIELD, transform(record.records, IndexRecord::getId)), record.parent);
        SearchResponse response = client.search(auth, filterToRequest(record.repo, idBuilder.build(), FETCH_SOURCE, immutableList(), record.size()));
        return uniqueIndex(DocResultBuilder.buildFrom(response, NONE), DocResult::keyString);
    }

    public static DocResult findByKey(ElasticClient client, EsAuth auth, String repo, String key) { return findByKey(client, auth, repo, key, null); }

    public static DocResult findByKey(ElasticClient client, EsAuth auth, String repo, String key, FetchSourceContext context) {
        return findBy(client, auth, repo, equalsTo(KEY_FIELD, key), context);
    }

    public static DocResult findById(ElasticClient client, EsAuth auth, String repo, String model, String id) {
        return findById(client, auth, repo, model, id, null, null);
    }

    public static DocResult findById(ElasticClient client, EsAuth auth, String repo, String model, String id, String parent, FetchSourceContext ctx) {
        return findBy(client, auth, repo, idBuilder(model, equalsTo(ID_FIELD, id), parent).build(), ctx);
    }

    @SneakyThrows private static DocResult findBy(ElasticClient client, EsAuth auth, String repo, Expression expr, FetchSourceContext context) {
        SearchResponse response = client.search(auth, filterToRequest(repo, expr, context, immutableList(), 1));
        return getReturnedHits(response) > 0 ? DocResultBuilder.buildFrom(response, NONE).get(0) : null;
    }

    private static BoolBuilder idBuilder(String model, Expression must, String parent) {
        BoolBuilder idBuilder = bool().liveOrPending().must(equalsTo(MODEL_FIELD, model)).must(must);
        if (parent != null) { idBuilder.filter(equalsTo(PARENT_FIELD, parent)); }
        return idBuilder;
    }
}