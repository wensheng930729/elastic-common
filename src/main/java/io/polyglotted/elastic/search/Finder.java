package io.polyglotted.elastic.search;

import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.DocResult;
import io.polyglotted.elastic.common.EsAuth;
import io.polyglotted.elastic.index.IndexRecord;
import io.polyglotted.elastic.search.Expressions.BoolBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.util.List;
import java.util.Map;

import static io.polyglotted.common.util.CollUtil.transform;
import static io.polyglotted.common.util.CollUtil.uniqueIndex;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.KEY_FIELD;
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

    public static Map<String, DocResult> findAll(ElasticClient client, EsAuth auth, String repo, String parent, List<IndexRecord> records) {
        BoolBuilder idBuilder = idBuilder(in(ID_FIELD, transform(records, IndexRecord::getId)), parent);
        SearchResponse response = client.search(auth, filterToRequest(repo, idBuilder.build(), FETCH_SOURCE, immutableList(), records.size()));
        return uniqueIndex(DocResultBuilder.buildFrom(response, NONE), DocResult::keyString);
    }

    public static DocResult findByKey(ElasticClient client, EsAuth auth, String repo, String key) { return findByKey(client, auth, repo, key, null); }

    public static DocResult findByKey(ElasticClient client, EsAuth auth, String repo, String key, FetchSourceContext context) {
        return findBy(client, auth, repo, equalsTo(KEY_FIELD, key), context);
    }

    public static DocResult findById(ElasticClient client, EsAuth auth, String repo, String id) { return findById(client, auth, repo, id, null, null); }

    public static DocResult findById(ElasticClient client, EsAuth auth, String repo, String id, String parent, FetchSourceContext context) {
        return findBy(client, auth, repo, idBuilder(equalsTo(ID_FIELD, id), parent).build(), context);
    }

    @SneakyThrows private static DocResult findBy(ElasticClient client, EsAuth auth, String repo, Expression expr, FetchSourceContext context) {
        SearchResponse response = client.search(auth, filterToRequest(repo, expr, context, immutableList(), 1));
        return getReturnedHits(response) > 0 ? DocResultBuilder.buildFrom(response, NONE).get(0) : null;
    }

    private static BoolBuilder idBuilder(Expression must, String parent) {
        BoolBuilder idBuilder = bool().liveIndex().must(must);
        if (parent != null) { idBuilder.filter(equalsTo(PARENT_FIELD, parent)); }
        return idBuilder;
    }
}