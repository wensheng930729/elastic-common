package io.polyglotted.elastic.test;

import io.polyglotted.elastic.search.Expression;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.elastic.search.ExprConverter.buildFilter;
import static io.polyglotted.elastic.search.Expressions.all;
import static io.polyglotted.elastic.search.Expressions.bool;
import static io.polyglotted.elastic.search.Expressions.equalsTo;
import static io.polyglotted.elastic.search.Expressions.exists;
import static io.polyglotted.elastic.search.Expressions.in;
import static io.polyglotted.elastic.search.Expressions.missing;
import static io.polyglotted.elastic.search.Expressions.notEquals;
import static io.polyglotted.elastic.search.Expressions.text;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(JUnitParamsRunner.class)
public class ExpressionsTest {
    public static Object[][] expressionInputs() {
        return new Object[][]{
            {bool().approvalRejected().build(), "{\"bool\":{\"filter\":[{\"terms\":{\"&status\":[\"REJECTED\"],\"boost\":1.0}}]," +
                "\"must_not\":[{\"exists\":{\"field\":\"&expiry\",\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}"},
            {bool().archiveIndex().build(), "{\"bool\":{\"filter\":[{\"exists\":{\"field\":\"&status\",\"boost\":1.0}}," +
                "{\"exists\":{\"field\":\"&expiry\",\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}"},
            {bool().liveIndex().build(), "{\"bool\":{\"filter\":[{\"exists\":{\"field\":\"&timestamp\",\"boost\":1.0}}]," +
                "\"must_not\":[{\"exists\":{\"field\":\"&status\",\"boost\":1.0}},{\"exists\":{\"field\":\"&expiry\"," +
                "\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}"},
            {bool().pendingApproval().build(), "{\"bool\":{\"filter\":[{\"terms\":{\"&status\":[\"PENDING\",\"PENDING_DELETE\"],\"boost\"" +
                ":1.0}}],\"must_not\":[{\"exists\":{\"field\":\"&expiry\",\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}"},
            {all(), "{\"match_all\":{\"boost\":1.0}}"},
            {bool().musts(exists("baz"), equalsTo("foo", "bar"), in("baz", immutableList(1, 2))).build(),
                "{\"bool\":{\"must\":[{\"exists\":{\"field\":\"baz\",\"boost\":1.0}},{\"term\":{\"foo\":{\"value\":\"bar\"," +
                    "\"boost\":1.0}}},{\"terms\":{\"baz\":[1,2],\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}"},
            {bool().shoulds(missing("baz"), notEquals("foo", "bar")).build(),
                "{\"bool\":{\"should\":[{\"bool\":{\"must_not\":[{\"exists\":{\"field\":\"baz\",\"boost\":1.0}}],\"adjust_pure_negative\"" +
                    ":true,\"boost\":1.0}},{\"bool\":{\"must_not\":[{\"term\":{\"foo\":{\"value\":\"bar\",\"boost\":1.0}}}],\"" +
                    "adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}"},
            {text("foo", "bar"), "{\"match_phrase_prefix\":{\"foo\":{\"query\":\"bar\",\"slop\":0,\"max_expansions\":50,\"boost\":1.0}}}"},
        };
    }

    @Test @Parameters(method = "expressionInputs")
    public void expressionBuildFrom(Expression expression, String json) throws Exception {
        QueryBuilder query = buildFilter(expression);
        XContentBuilder result = XContentFactory.jsonBuilder();
        query.toXContent(result, ToXContent.EMPTY_PARAMS);
        assertThat(result.string(), result.string(), is(json));
    }
}