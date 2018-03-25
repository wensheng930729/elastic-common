package io.polyglotted.elastic.search;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.util.ListBuilder.ImmutableListBuilder;
import io.polyglotted.common.util.MapBuilder;
import io.polyglotted.common.util.MapRetriever;
import io.polyglotted.elastic.common.MetaFields;
import io.polyglotted.elastic.common.Verbose;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

import java.util.List;
import java.util.Map;

import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.util.CollUtil.filterKeys;
import static io.polyglotted.common.util.ListBuilder.immutableListBuilder;
import static io.polyglotted.elastic.common.MetaFields.HIGHLTGHT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("unused")
public interface ResultBuilder<T> extends ResponseBuilder<T> {

    default List<T> buildFrom(SearchResponse response, Verbose verbose) {
        ImmutableListBuilder<T> builder = immutableListBuilder();
        for (SearchHit hit : response.getHits()) { builder.add(buildFromHit(hit, verbose)); }
        return builder.build();
    }

    default T buildFromHit(SearchHit hit, Verbose verbose) {
        T result = buildVerbose(requireNonNull(hit).hasSource() ? simpleResult(hit.getSourceAsMap()) : simpleResult(), verbose);
        Map<String, HighlightField> highlightFields = hit.getHighlightFields();
        if (result instanceof MapResult && !highlightFields.isEmpty()) {

            Multimap<String, String> highlight = ArrayListMultimap.create();
            for (Map.Entry<String, HighlightField> entry : highlightFields.entrySet()) {
                HighlightField field = entry.getValue();
                for (Text frag : field.fragments()) { highlight.put(field.name(), frag.string()); }
            }
            MapResult.class.cast(result).put(HIGHLTGHT_FIELD, highlight.asMap());
        }
        return result;
    }

    default T buildVerbose(MapResult source, Verbose verbose) {
        if (source == null || source.isEmpty()) return null;
        return verbose.buildFrom(source, buildResult(source));
    }

    T buildResult(MapResult source);

    ResultBuilder<?> NullBuilder = MapBuilder::immutableMap;
    ResultBuilder<MapResult> HeaderBuilder = MetaFields::readHeader;
    ResultBuilder<String> IdBuilder = (source) -> MapRetriever.reqdStr(source, ID_FIELD);
    ResultBuilder<MapResult> EmptyBuilder = MapResult::simpleResult;
    ResultBuilder<MapResult> SourceBuilder = (source) -> simpleResult(filterKeys(source, MetaFields::isNotMeta));
}