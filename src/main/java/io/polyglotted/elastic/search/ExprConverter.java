package io.polyglotted.elastic.search;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.search.MatchQuery;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Iterables.toArray;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.elastic.search.Expression.NilExpression;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhrasePrefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;

@SuppressWarnings("unused")
public enum ExprConverter {
    All {
        @Override
        QueryBuilder buildFrom(Expression expr) { return matchAllQuery(); }
    },
    Bool {
        @Override
        QueryBuilder buildFrom(Expression expr) {
            BoolQueryBuilder result = boolQuery();
            for (QueryBuilder child : transform(expr.<List<Expression>>argFor("must"), ExprConverter::buildFilter)) {
                if (child != null) { result.must(child); }
            }
            for (QueryBuilder child : transform(expr.<List<Expression>>argFor("filter"), ExprConverter::buildFilter)) {
                if (child != null) { result.filter(child); }
            }
            for (QueryBuilder child : transform(expr.<List<Expression>>argFor("should"), ExprConverter::buildFilter)) {
                if (child != null) { result.should(child); }
            }
            for (QueryBuilder child : transform(expr.<List<Expression>>argFor("must_not"), ExprConverter::buildFilter)) {
                if (child != null) { result.mustNot(child); }
            }
            return result;
        }
    },
    Eq {
        @Override
        QueryBuilder buildFrom(Expression expr) { return termQuery(expr.label, (Object) expr.valueArg()); }
    },
    Ne {
        @Override
        QueryBuilder buildFrom(Expression expr) { return boolQuery().mustNot(termQuery(expr.label, (Object) expr.valueArg())); }
    },
    In {
        @Override
        QueryBuilder buildFrom(Expression expr) { return termsQuery(expr.label, expr.arrayArg()); }
    },
    Text {
        @Override
        QueryBuilder buildFrom(Expression expr) {
            String field = isNullOrEmpty(expr.label) ? "_all" : expr.label;
            Operator operator = expr.args.containsKey("operator") ? Operator.valueOf(expr.stringArg("operator")) : Operator.AND;
            MatchQuery.Type type = MatchQuery.Type.valueOf(nonNull(expr.stringArg("type"), "PHRASE_PREFIX"));
            switch (type) {
                case BOOLEAN:
                    return matchQuery(field, expr.valueArg()).operator(operator).analyzer(expr.stringArg("analyzer"));
                case PHRASE:
                    return matchPhraseQuery(field, expr.valueArg()).analyzer(expr.stringArg("analyzer"));
                case PHRASE_PREFIX:
                default:
                    return matchPhrasePrefixQuery(field, expr.valueArg()).analyzer(expr.stringArg("analyzer"));
            }
        }
    },
    Exists {
        @Override
        QueryBuilder buildFrom(Expression expr) { return existsQuery(expr.label); }
    },
    Missing {
        @Override
        QueryBuilder buildFrom(Expression expr) { return boolQuery().mustNot(existsQuery(expr.label)); }
    };

    abstract QueryBuilder buildFrom(Expression expr);

    public static QueryBuilder buildFilter(Expression expr) {
        return expr == null || NilExpression.equals(expr) ? null : valueOf(expr.operation).buildFrom(expr);
    }

    @SuppressWarnings("ConstantConditions") private static QueryBuilder[] aggregateFilters(Collection<Expression> expressions) {
        return toArray(filter(transform(expressions, ExprConverter::buildFilter), Objects::nonNull), QueryBuilder.class);
    }
}