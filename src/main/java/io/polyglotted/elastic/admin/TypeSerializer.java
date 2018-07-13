package io.polyglotted.elastic.admin;

import io.polyglotted.common.util.ListBuilder.ImmutableListBuilder;
import lombok.SneakyThrows;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static io.polyglotted.common.util.CollUtil.concat;
import static io.polyglotted.common.util.CollUtil.uniqueIndex;
import static io.polyglotted.common.util.ListBuilder.immutableListBuilder;
import static io.polyglotted.common.util.ListBuilder.immutableSortedSet;
import static io.polyglotted.elastic.admin.Field.fieldBuilder;
import static io.polyglotted.elastic.admin.Field.keywordField;
import static io.polyglotted.elastic.admin.Field.simpleField;
import static io.polyglotted.elastic.admin.Field.textField;
import static io.polyglotted.elastic.admin.FieldType.DATE;
import static io.polyglotted.elastic.admin.FieldType.JOIN;
import static io.polyglotted.elastic.admin.FieldType.OBJECT;
import static io.polyglotted.elastic.common.MetaFields.ALL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.ANCESTOR_FIELD;
import static io.polyglotted.elastic.common.MetaFields.APPROVAL_ROLES_FIELD;
import static io.polyglotted.elastic.common.MetaFields.AUTO_COMPLETE_FIELD;
import static io.polyglotted.elastic.common.MetaFields.COMMENT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.EXPIRY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.KEY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.LINK_FIELD;
import static io.polyglotted.elastic.common.MetaFields.MODEL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.PARENT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.REALM_FIELD;
import static io.polyglotted.elastic.common.MetaFields.STATUS_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TIMESTAMP_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TRAITFQN_FIELD;
import static io.polyglotted.elastic.common.MetaFields.UNIQUE_FIELD;
import static io.polyglotted.elastic.common.MetaFields.UPDATER_FIELD;
import static io.polyglotted.elastic.common.MetaFields.USER_FIELD;

@SuppressWarnings("WeakerAccess")
public abstract class TypeSerializer {

    @SneakyThrows public static String serializeType(Type type) { return writeType(type, XContentFactory.jsonBuilder()).string(); }

    private static XContentBuilder writeType(Type type, XContentBuilder gen) throws IOException {
        gen.startObject();
        if (type.strict) gen.field("dynamic", "strict");
        if (!type.enabled) { gen.field("enabled", false); }
        else {
            if (!type.meta.isEmpty()) { gen.field("_meta", type.meta); }
            if (!type.enableSource) { gen.field("_source").startObject().field("enabled", false).endObject(); }
            else {
                if (type.enableSize) { gen.field("_size").startObject().field("enabled", true).endObject(); }
                if (type.includeMeta) { gen.field("_source").startObject().field("excludes", type.sourceExcludes()).endObject(); }
                writeFields("properties", uniqueIndex(typeValues(type), Field::field), gen);
            }
        }
        return gen.endObject();
    }

    private static Set<Field> typeValues(Type type) {
        return type.includeMeta ? immutableSortedSet(concat(metaFields(type), type.fields)) : immutableSortedSet(type.fields);
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

    private static List<Field> metaFields(Type type) {
        ImmutableListBuilder<Field> builder = immutableListBuilder();
        if (type.enableAll) { builder.add(textField(ALL_FIELD, "all").build()); }
        if (type.enableAutocomplete) { builder.add(textField(AUTO_COMPLETE_FIELD, "autocomplete").build()); }
        if (type.hasApproval) {
            builder.add(keywordField(APPROVAL_ROLES_FIELD).build()).add(textField(COMMENT_FIELD, "all").build());
        }
        builder.add(type.hasRelations() ? fieldBuilder(MODEL_FIELD, JOIN).arg("relations", type.relations).build()
            : keywordField(MODEL_FIELD).normalise().build());
        builder.add(keywordField(ANCESTOR_FIELD).build());
        builder.add(simpleField(EXPIRY_FIELD, DATE).build());
        builder.add(keywordField(ID_FIELD).normalise().build());
        builder.add(keywordField(KEY_FIELD).normalise().build());
        builder.add(keywordField(LINK_FIELD).build());
        builder.add(keywordField(PARENT_FIELD).build());
        builder.add(keywordField(REALM_FIELD).build());
        builder.add(keywordField(STATUS_FIELD).build());
        builder.add(simpleField(TIMESTAMP_FIELD, DATE).build());
        builder.add(keywordField(TRAITFQN_FIELD).build());
        builder.add(keywordField(UNIQUE_FIELD).normalise().build());
        builder.add(keywordField(UPDATER_FIELD).build());
        builder.add(keywordField(USER_FIELD).build());
        return builder.build();
    }
}