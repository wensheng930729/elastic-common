package io.polyglotted.elastic.common;

import com.fasterxml.jackson.core.JsonGenerator;
import io.polyglotted.common.model.MapResult;
import lombok.SneakyThrows;

import java.io.StringWriter;
import java.util.Map;

import static io.polyglotted.common.util.BaseSerializer.FACTORY;
import static io.polyglotted.common.util.BaseSerializer.serialize;

public interface HasMeta {
    MapResult _meta();

    default boolean hasMeta() { return !_meta().isEmpty(); }

    @SneakyThrows static <T extends HasMeta> String serializeMeta(T holder) {
        StringWriter writer = new StringWriter();
        try (JsonGenerator gen = FACTORY.createGenerator(writer)) {
            gen.writeStartObject();
            for (Map.Entry<String, Object> meta : holder._meta().entrySet()) {
                gen.writeObjectField(meta.getKey(), meta.getValue());
            }
            if (holder.hasMeta()) { gen.writeRaw(","); }
            String serialized = serialize(holder);
            gen.writeRaw(serialized.substring(1, serialized.length() - 1));
            gen.writeEndObject();
        }
        return writer.toString();
    }
}