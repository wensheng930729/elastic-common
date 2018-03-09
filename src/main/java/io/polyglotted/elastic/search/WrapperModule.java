package io.polyglotted.elastic.search;

import lombok.SneakyThrows;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.matrix.MatrixAggregationPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static io.polyglotted.common.util.ListBuilder.immutableList;
import static org.elasticsearch.common.settings.Settings.EMPTY;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.elasticsearch.search.builder.SearchSourceBuilder.fromXContent;

/**
 * this is a derived from org.elasticsearch.search.SearchModule
 */
final class WrapperModule {
    private final NamedXContentRegistry contentRegistry;

    WrapperModule() {
        this.contentRegistry = new NamedXContentRegistry(new SearchModule(EMPTY, false,
            immutableList(new ParentJoinPlugin(), new MatrixAggregationPlugin())).getNamedXContents());
    }

    @SneakyThrows SearchSourceBuilder sourceFrom(byte[] bytes) { return fromXContent(JSON.xContent().createParser(contentRegistry, bytes)); }
}