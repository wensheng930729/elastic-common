package io.polyglotted.elastic.index;

import io.polyglotted.common.model.MapResult;
import lombok.SneakyThrows;

import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.MODEL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.RESULT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TIMESTAMP_FIELD;
import static io.polyglotted.elastic.common.MetaFields.model;
import static io.polyglotted.elastic.common.MetaFields.reqdId;
import static io.polyglotted.elastic.common.MetaFields.timestamp;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@SuppressWarnings({"serial", "WeakerAccess"}) final class NoopException extends RuntimeException {
    NoopException(MapResult current) { super(buildMessage(current)); }

    @SneakyThrows private static String buildMessage(MapResult current) {
        return jsonBuilder().startObject().field(MODEL_FIELD, model(current)).field(ID_FIELD, reqdId(current))
            .field(TIMESTAMP_FIELD, timestamp(current)).field(RESULT_FIELD, "noop").endObject().string();
    }
}