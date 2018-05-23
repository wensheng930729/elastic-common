package io.polyglotted.elastic.test;

import io.polyglotted.elastic.common.Verbose;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.common.util.ListBuilder.simpleList;
import static io.polyglotted.elastic.common.MetaFields.ALL_FIELDS;
import static io.polyglotted.elastic.common.Verbose.ID;
import static io.polyglotted.elastic.common.Verbose.KEY;
import static io.polyglotted.elastic.common.Verbose.META;
import static io.polyglotted.elastic.common.Verbose.MINIMAL;
import static io.polyglotted.elastic.common.Verbose.NONE;
import static io.polyglotted.elastic.common.Verbose.UNIQUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(JUnitParamsRunner.class)
public class VerboseTest {

    public static Object[][] verboseInputs() {
        return new Object[][]{
            {NONE, ALL_FIELDS},
            {ID, simpleAllList(ID.fields)},
            {KEY, simpleAllList(KEY.fields)},
            {MINIMAL, simpleAllList(MINIMAL.fields)},
            {UNIQUE, simpleAllList(UNIQUE.fields)},
            {META, immutableList()},
        };
    }

    @Test @Parameters(method = "verboseInputs")
    public void fetchContextSuccess(Verbose verbose, List<String> expected) {
        assertThat(immutableList(verbose.fetchContext.excludes()), is(expected));
    }

    private static List<String> simpleAllList(String... removes) {
        List<String> all = simpleList(ALL_FIELDS); all.removeAll(Arrays.asList(removes)); return all;
    }
}