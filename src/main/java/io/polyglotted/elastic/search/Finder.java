package io.polyglotted.elastic.search;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.DocResult;
import io.polyglotted.elastic.index.BulkRecord;
import io.polyglotted.elastic.index.IndexRecord;
import io.polyglotted.elastic.search.Expressions.BoolBuilder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.util.List;
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

    public static Map<String, DocResult> findAll(ElasticClient client, AuthHeader auth, BulkRecord record) {
        BoolBuilder builder = idBuilder(record.model, record.parent, in(ID_FIELD, transform(record.records, IndexRecord::getId))).liveOrPending();
        return uniqueIndex(findAllBy(client, auth, record.repo, builder.build(), record.size(), FETCH_SOURCE), DocResult::keyString);
    }

    public static List<DocResult> findAllBy(ElasticClient client, AuthHeader auth, String repo, Expression expr, int size, FetchSourceContext ctx) {
        SearchRequest searchRequest = filterToRequest(repo, expr, ctx, immutableList(), size);
        SearchResponse response = auth == null ? client.search(searchRequest) : client.search(auth, searchRequest);
        return DocResultBuilder.buildFrom(response, NONE);
    }

    public static DocResult findByKey(ElasticClient client, AuthHeader auth, String repo, String key) { return findByKey(client, auth, repo, key, null); }

    public static DocResult findByKey(ElasticClient client, AuthHeader auth, String repo, String key, FetchSourceContext context) {
        return findBy(client, auth, repo, equalsTo(KEY_FIELD, key), context);
    }

    public static DocResult findById(ElasticClient client, AuthHeader auth, String repo, String model, String id) {
        return findById(client, auth, repo, model, id, null, null);
    }

    public static DocResult findById(ElasticClient client, AuthHeader auth, String repo, String model, String id, String parent, FetchSourceContext ctx) {
        return findBy(client, auth, repo, idBuilder(model, parent, equalsTo(ID_FIELD, id)).liveOrPending().build(), ctx);
    }

    public static DocResult findBy(ElasticClient client, AuthHeader auth, String repo, Expression expr, FetchSourceContext context) {
        SearchRequest searchRequest = filterToRequest(repo, expr, context, immutableList(), 1);
        SearchResponse response = auth == null ? client.search(searchRequest) : client.search(auth, searchRequest);
        return getReturnedHits(response) > 0 ? DocResultBuilder.buildFrom(response, NONE).get(0) : null;
    }

    public static BoolBuilder idBuilder(String model, Expression... musts) { return idBuilder(model, null, musts); }

    public static BoolBuilder idBuilder(String model, String parent, Expression... musts) {
        BoolBuilder idBuilder = bool().must(equalsTo(MODEL_FIELD, model)).musts(musts);
        if (parent != null) { idBuilder.filter(equalsTo(PARENT_FIELD, parent)); }
        return idBuilder;
    }
}