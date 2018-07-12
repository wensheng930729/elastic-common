package io.polyglotted.elastic.test;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.common.MetaFields;
import io.polyglotted.elastic.common.Verbose;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.polyglotted.common.model.MapResult.immutableResult;
import static io.polyglotted.common.model.MapResult.immutableResultBuilder;
import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.model.MapResult.simpleResultBuilder;
import static io.polyglotted.elastic.common.Verbose.ID;
import static io.polyglotted.elastic.common.Verbose.KEY;
import static io.polyglotted.elastic.common.Verbose.META;
import static io.polyglotted.elastic.common.Verbose.MINIMAL;
import static io.polyglotted.elastic.common.Verbose.NONE;
import static io.polyglotted.elastic.common.Verbose.UNIQUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(JUnitParamsRunner.class)
public class MetaFieldsTest extends MetaFields {
    public static Object[][] readInputs() {
        return new Object[][]{
            {NONE, immutableResult()},
            {ID, immutableResult(ID_FIELD, "i1")},
            {KEY, immutableResult(KEY_FIELD, "M1:i1:1000")},
            {MINIMAL, immutableResult(ID_FIELD, "i1", KEY_FIELD, "M1:i1:1000", MODEL_FIELD, "M1", TIMESTAMP_FIELD, 1000L)},
            {UNIQUE, immutableResult(ID_FIELD, "i1", KEY_FIELD, "M1:i1:1000")},
            {META, immutableResultBuilder().put(ID_FIELD, "i1").put(KEY_FIELD, "M1:i1:1000").put("&lineageKey", "fooBar")
                .put(MODEL_FIELD, "M1").put(TIMESTAMP_FIELD, 1000L).put("&taskId", "baz1").result()},
        };
    }

    @Test @Parameters(method = "readInputs")
    public void readHeaderTest(Verbose verbose, MapResult expected) {
        MapResult source = simpleResultBuilder().put("&model", "M1").put("&id", "i1")
            .put("&timestamp", 1000L).put("&key", "M1:i1:1000").put("&lineageKey", "fooBar").put("&taskId", "baz1").result();

        assertThat(verbose.buildFrom(source, simpleResult()), is(expected));
    }
}