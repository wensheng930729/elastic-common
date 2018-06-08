package io.polyglotted.elastic.admin;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.MapBuilder;
import io.polyglotted.common.util.TokenUtil;
import lombok.SneakyThrows;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Map;

import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.common.util.BaseSerializer.serialize;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.common.util.MapRetriever.optStr;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.elastic.admin.Type.TYPE_DOC;
import static io.polyglotted.elastic.search.ExprConverter.buildFilter;
import static io.polyglotted.elastic.search.Expressions.allIndex;
import static io.polyglotted.elastic.search.Expressions.approvalRejected;
import static io.polyglotted.elastic.search.Expressions.archiveIndex;
import static io.polyglotted.elastic.search.Expressions.liveIndex;
import static io.polyglotted.elastic.search.Expressions.pendingApproval;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class IndexRequestor {
    private static final Map<String, QueryBuilder> FILTER_BUILDERS = MapBuilder.<String, QueryBuilder>immutableMapBuilder()
        .put("ALL", buildFilter(allIndex())).put("LIVE", buildFilter(liveIndex())).put("ARCHIVE", buildFilter(archiveIndex()))
        .put("PENDING", buildFilter(pendingApproval())).put("REJECTED", buildFilter(approvalRejected())).build();

    public static String indexName(IndexSetting setting) { return nonNull(optStr(setting.mapResult, "index_name"), TokenUtil::uniqueToken); }

    public static Alias aliasFrom(String name, String filter) { return new Alias(name).filter(FILTER_BUILDERS.get(filter)); }

    public static String indexFile(IndexSetting setting, Type type, String alias) { return indexFile(setting, type, alias, immutableList()); }

    @SneakyThrows public static String indexFile(IndexSetting setting, Type mapping, String writeAlias, List<Alias> readAliases) {
        CreateIndexRequest request = createIndexRequest(indexName(setting))
            .updateAllTypes(true).settings(setting.toJson(), JSON).mapping(TYPE_DOC, mapping.toJson(), JSON);
        if (writeAlias != null) { request.alias(new Alias(writeAlias)); }
        for (Alias alias : readAliases) { request.alias(alias); }

        XContentBuilder builder = XContentFactory.jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.close();
        return builder.string();
    }

    public static String templateFile(IndexSetting setting, Type type, String alias) { return templateFile(setting, type, alias, immutableList()); }

    @SneakyThrows public static String templateFile(IndexSetting setting, Type mapping, String writeAlias, List<Alias> readAliases) {
        MapResult result = deserialize(indexFile(setting, mapping, null, readAliases));
        result.put("index_patterns", immutableList(writeAlias));
        return serialize(result);
    }
}