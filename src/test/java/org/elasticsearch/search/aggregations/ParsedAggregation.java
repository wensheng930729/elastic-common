package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

//DIRTIEST TRICK IN THE JAVA BOOK!!
@SuppressWarnings({"unused", "WeakerAccess", "UnusedReturnValue"})
public abstract class ParsedAggregation implements Aggregation, ToXContentFragment {

    protected static void declareAggregationFields(ObjectParser<? extends ParsedAggregation, Void> objectParser) {
        objectParser.declareObject((parsedAgg, metadata) -> parsedAgg.metadata = Collections.unmodifiableMap(metadata),
            (parser, context) -> parser.map(), InternalAggregation.CommonFields.META);
    }

    private String name;
    protected Map<String, Object> metadata;

    @Override
    public final String getName() { return name; }

    protected void setName(String name) { this.name = name; }

    @Override
    public final Map<String, Object> getMetaData() { return metadata; }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // FRIKKIN MISTAKE FROM THE ORIGINAL FILE THAT WE WANT TO FIX!!
        builder.startObject(name);
        if (this.metadata != null) {
            builder.field(InternalAggregation.CommonFields.META.getPreferredName());
            builder.map(this.metadata);
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    protected static double parseDouble(XContentParser parser, double defaultNullValue) throws IOException {
        XContentParser.Token currentToken = parser.currentToken();
        if (currentToken == XContentParser.Token.VALUE_NUMBER || currentToken == XContentParser.Token.VALUE_STRING) {
            return parser.doubleValue();
        } else {
            return defaultNullValue;
        }
    }
}