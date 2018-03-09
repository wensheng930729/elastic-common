package io.polyglotted.elastic.admin;

import lombok.SneakyThrows;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.Sets.newTreeSet;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.elastic.admin.Field.keywordField;
import static io.polyglotted.elastic.admin.Field.simpleField;
import static io.polyglotted.elastic.admin.Field.textField;
import static io.polyglotted.elastic.admin.FieldType.DATE;
import static io.polyglotted.elastic.admin.FieldType.OBJECT;
import static io.polyglotted.elastic.common.MetaFields.ALL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.ANCESTOR_FIELD;
import static io.polyglotted.elastic.common.MetaFields.AUTO_COMPLETE_FIELD;
import static io.polyglotted.elastic.common.MetaFields.EXPIRY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.KEY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.LINK_FIELD;
import static io.polyglotted.elastic.common.MetaFields.MODEL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.PARENT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.SIZE_FIELD;
import static io.polyglotted.elastic.common.MetaFields.STATUS_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TIMESTAMP_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TRAITFQN_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TRAITID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.UNIQUE_FIELD;
import static io.polyglotted.elastic.common.MetaFields.UPDATER_FIELD;
import static io.polyglotted.elastic.common.MetaFields.USER_FIELD;

public abstract class TypeSerializer {

    @SneakyThrows static String serializeType(Type type) { return writeType(type, XContentFactory.jsonBuilder()).string(); }

    private static XContentBuilder writeType(Type type, XContentBuilder gen) throws IOException {
        gen.startObject().field(type.type).startObject();
        if (type.strict) gen.field("dynamic", "strict");
        if (!type.enabled) { gen.field("enabled", false); }
        else {
            if (!type.meta.isEmpty()) { gen.field("_meta", type.meta); }
            gen.field("_all").startObject().field("enabled", false).endObject();
            if (type.parent != null) {
                gen.field("_parent").startObject().field("type", type.parent).endObject();
                gen.field("_routing").startObject().field("required", true).endObject();
            }
            if (!type.enableSource) { gen.field("_source").startObject().field("enabled", false).endObject(); }
            else {
                if (type.includeMeta) { gen.field("_source").startObject().field("excludes", type.sourceExcludes()).endObject(); }
                writeFields("properties", uniqueIndex(typeValues(type), Field::field), gen);
            }
        }
        return gen.endObject().endObject();
    }

    private static TreeSet<Field> typeValues(Type type) {
        return type.includeMeta ? newTreeSet(concat(metaFields(), type.fields)) : newTreeSet(type.fields);
    }

    private static void writeFields(String name, Map<String, Field> fields, XContentBuilder gen) throws IOException {
        gen.field(name).startObject();
        for (Entry<String, Field> entry : fields.entrySet()) { gen.field(entry.getKey()); writeField(entry.getValue(), gen); }
        gen.endObject();
    }

    private static void writeField(Field field, XContentBuilder gen) throws IOException {
        gen.startObject();
        if (field.type != OBJECT) { gen.field("type", field.type.validate(field)); }
        else if (!field.hasProperties()) { gen.field("type", OBJECT.validate(field)); }
        if (field.hasFields()) { writeFields("fields", field.properties, gen); }
        writeNotNullOrEmpty(gen, "index", field.indexed);
        writeNotNullOrEmpty(gen, "store", field.stored);
        writeNotNullOrEmpty(gen, "doc_values", field.docValues);
        writeNotNullOrEmpty(gen, "analyzer", field.analyzer);
        if (field.copyToFields.size() > 0) gen.field("copy_to", field.copyToFields);
        for (Entry<String, Object> arg : field.argsMap.entrySet()) { writeNotNullOrEmpty(gen, arg.getKey(), arg.getValue()); }
        if (field.hasProperties()) { writeFields("properties", field.properties, gen); }
        gen.endObject();
    }

    private static void writeNotNullOrEmpty(XContentBuilder builder, String key, Object value) throws IOException {
        if (value != null) { builder.field(key, value); }
    }

    private static List<Field> metaFields() {
        return immutableList(
            textField(ALL_FIELD, "allAnalyzer").build(),
            keywordField(ANCESTOR_FIELD).build(),
            textField(AUTO_COMPLETE_FIELD, "autoAnalyzer").build(),
            simpleField(EXPIRY_FIELD, DATE).build(),
            keywordField(ID_FIELD).normalise().build(),
            keywordField(KEY_FIELD).normalise().build(),
            keywordField(LINK_FIELD).build(),
            keywordField(MODEL_FIELD).normalise().build(),
            keywordField(PARENT_FIELD).build(),
            keywordField(SIZE_FIELD).build(),
            keywordField(STATUS_FIELD).build(),
            simpleField(TIMESTAMP_FIELD, DATE).build(),
            keywordField(TRAITFQN_FIELD).build(),
            keywordField(TRAITID_FIELD).build(),
            keywordField(UNIQUE_FIELD).normalise().build(),
            keywordField(UPDATER_FIELD).build(),
            keywordField(USER_FIELD).build());
    }
}