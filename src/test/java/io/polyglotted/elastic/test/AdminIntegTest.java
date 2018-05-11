package io.polyglotted.elastic.test;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.admin.IndexSetting;
import io.polyglotted.elastic.admin.Type;
import io.polyglotted.elastic.client.ElasticClient;
import org.junit.Test;

import java.util.Map;

import static io.polyglotted.common.model.MapResult.immutableResult;
import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.common.util.BaseSerializer.serialize;
import static io.polyglotted.common.util.CollUtil.filterKeys;
import static io.polyglotted.common.util.CollUtil.firstOf;
import static io.polyglotted.common.util.MapBuilder.immutableMap;
import static io.polyglotted.common.util.MapRetriever.deepRetrieve;
import static io.polyglotted.common.util.ResourceUtil.readResourceAsMap;
import static io.polyglotted.elastic.admin.Field.keywordField;
import static io.polyglotted.elastic.admin.Field.nestedField;
import static io.polyglotted.elastic.admin.Field.nonIndexField;
import static io.polyglotted.elastic.admin.Field.objectField;
import static io.polyglotted.elastic.admin.Field.simpleField;
import static io.polyglotted.elastic.admin.Field.textField;
import static io.polyglotted.elastic.admin.FieldType.BINARY;
import static io.polyglotted.elastic.admin.FieldType.BOOLEAN;
import static io.polyglotted.elastic.admin.FieldType.BYTE;
import static io.polyglotted.elastic.admin.FieldType.DATE;
import static io.polyglotted.elastic.admin.FieldType.DOUBLE;
import static io.polyglotted.elastic.admin.FieldType.FLOAT;
import static io.polyglotted.elastic.admin.FieldType.GEO_POINT;
import static io.polyglotted.elastic.admin.FieldType.GEO_SHAPE;
import static io.polyglotted.elastic.admin.FieldType.HALF_FLOAT;
import static io.polyglotted.elastic.admin.FieldType.INTEGER;
import static io.polyglotted.elastic.admin.FieldType.IP;
import static io.polyglotted.elastic.admin.FieldType.KEYWORD;
import static io.polyglotted.elastic.admin.FieldType.LONG;
import static io.polyglotted.elastic.admin.FieldType.SCALED_FLOAT;
import static io.polyglotted.elastic.admin.FieldType.SHORT;
import static io.polyglotted.elastic.admin.FieldType.TEXT;
import static io.polyglotted.elastic.admin.IndexSetting.settingBuilder;
import static io.polyglotted.elastic.admin.Type.typeBuilder;
import static io.polyglotted.elastic.test.ElasticTestUtil.testElasticClient;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class AdminIntegTest {
    private static final Map<String, String> MESSAGES = readResourceAsMap(AdminIntegTest.class, "admin-integ.txt");

    @Test
    public void createIndexSuccess() throws Exception {
        try (ElasticClient client = testElasticClient()) {
            IndexSetting setting = settingBuilder(5, 1).all(immutableResult("mapping.total_fields.limit", 5000)).build();
            Type completeType = completeTypeMapping().build();
            String index = client.createIndex(setting, completeType, "MyBigIndex");
            try {
                MapResult deserialized = deserialize(client.getSettings(index));

                Map<String, Object> settings = deepRetrieve(firstOf(deserialized.values(), immutableMap()), "settings.index");
                String serSetting = serialize(filterKeys(settings, asList("number_of_shards", "number_of_replicas", "mapping", "analysis")::contains));
                assertThat(serSetting, serSetting, is(MESSAGES.get("completeSetting")));

                String serMapping = serialize(client.getMapping(index));
                assertThat(serMapping, serMapping, is(MESSAGES.get("completeMapping")));
            } finally { client.dropIndex(index); }
        }
    }

    @Test
    public void parentChildIndexSuccess() throws Exception {
        try (ElasticClient client = testElasticClient()) {
            String index = client.createIndex(IndexSetting.with(2, 0), parentChildTypeMapping().build(), "ParChilIndex");
            try {
                String parChildMapping = serialize(client.getMapping(index));
                assertThat(parChildMapping, parChildMapping, is(MESSAGES.get("parentChildMapping")));
            } finally { client.dropIndex(index); }
        }
    }

    private static Type.Builder parentChildTypeMapping() {
        return typeBuilder().strict().enableAutocomplete(false).relation("Foo", asList("Bar", "Baz", "Tux"))
            .field(simpleField("val", KEYWORD));
    }

    private static Type.Builder completeTypeMapping() {
        return typeBuilder().strict()
            .metaData("myName", "myVal")
            .field(simpleField("binField", BINARY))
            .field(simpleField("boolField", BOOLEAN))
            .field(simpleField("dateField", DATE))
            .field(simpleField("geoPointField", GEO_POINT))
            .field(simpleField("geoShapeField", GEO_SHAPE).quadtree())
            .field(simpleField("ipField", IP))
            .field(simpleField("doubleField", DOUBLE))
            .field(simpleField("floatField", FLOAT))
            .field(simpleField("halfFloatField", HALF_FLOAT))
            .field(simpleField("scaledFloatField", SCALED_FLOAT).scaling(100.0))
            .field(simpleField("byteField", BYTE))
            .field(simpleField("shortField", SHORT))
            .field(simpleField("intField", INTEGER))
            .field(simpleField("longField", LONG))
            .field(keywordField("keyField"))
            .field(keywordField("pathField").isAPath().copyTo("copyField"))
            .field(nonIndexField("nonIndexedText", TEXT).copyTo("copyField"))
            .field(textField("textField", "standard"))
            .field(textField("rawField", "standard").addRawFields())
            .field(textField("copyField", "all"))
            .field(nestedField("emptyNestedField"))
            .field(nestedField("fullyNested").properties(asList(keywordField("effect"),
                nestedField("constraint").properties(asList(keywordField("attr").copyTo("copyField"), keywordField("func"))))))
            .field(objectField("objectField").mapping(keywordField("inner2")))
            .field(objectField("objectFieldEmpty"));
    }
}