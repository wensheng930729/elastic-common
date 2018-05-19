package io.polyglotted.elastic.search;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

import static io.polyglotted.common.model.MapResult.immutableResult;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.elastic.common.MetaFields.EXPIRY_FIELD;
import static io.polyglotted.elastic.common.MetaFields.STATUS_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TIMESTAMP_FIELD;
import static io.polyglotted.elastic.search.Expression.ValueKey;
import static io.polyglotted.elastic.search.Expression.withArray;
import static io.polyglotted.elastic.search.Expression.withLabel;
import static io.polyglotted.elastic.search.Expression.withMap;
import static io.polyglotted.elastic.search.Expression.withValue;

@SuppressWarnings({"WeakerAccess", "UnusedReturnValue"})
public abstract class Expressions {

    public static BoolBuilder bool() { return new BoolBuilder(); }

    public static Expression exists(String field) { return withLabel("Exists", field); }

    public static Expression missing(String field) { return withLabel("Missing", field); }

    public static Expression all() { return withLabel("All", ""); }

    public static Expression allIndex() { return bool().allIndex().build(); }

    public static Expression archiveIndex() { return bool().archiveIndex().build(); }

    public static Expression pendingApproval() { return bool().pendingApproval().build(); }

    public static Expression approvalRejected() { return bool().rejected().build(); }

    public static Expression liveIndex() { return bool().liveIndex().build(); }

    public static Expression equalsTo(String field, Object value) { return value == null ? missing(field) : withValue("Eq", field, value); }

    public static Expression notEquals(String field, Object value) { return value == null ? exists(field) : withValue("Ne", field, value); }

    public static Expression text(String field, Object value) { return withMap("Text", nonNull(field, ""), immutableResult(ValueKey, value)); }

    public static <E extends Comparable<E>> Expression in(String field, Iterable<E> values) { return withArray("In", field, immutableList(values)); }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class BoolBuilder {
        private final List<Expression> musts = new ArrayList<>();
        private final List<Expression> filters = new ArrayList<>();
        private final List<Expression> shoulds = new ArrayList<>();
        private final List<Expression> mustNots = new ArrayList<>();

        public BoolBuilder allIndex() { return filter(exists(TIMESTAMP_FIELD)); }

        public BoolBuilder rejected() { return filter(in(STATUS_FIELD, immutableList("REJECTED"))).not(exists(EXPIRY_FIELD)); }

        public BoolBuilder discarded() { return filter(equalsTo(STATUS_FIELD, "DISCARDED")); }

        public BoolBuilder pendingApproval() { return filter(in(STATUS_FIELD, immutableList("PENDING", "PENDING_DELETE"))).not(exists(EXPIRY_FIELD)); }

        public BoolBuilder archiveIndex() { return filters(exists(STATUS_FIELD), exists(EXPIRY_FIELD)); }

        public BoolBuilder liveIndex() { return filter(exists(TIMESTAMP_FIELD)).nots(exists(STATUS_FIELD), exists(EXPIRY_FIELD)); }

        public BoolBuilder liveOrPending(boolean apply) { return apply ? liveOrPending() : this; }

        public BoolBuilder liveOrPending() {
            return must(exists(TIMESTAMP_FIELD)).not(exists(EXPIRY_FIELD)).should(bool().not(exists(STATUS_FIELD)))
                .should(in(STATUS_FIELD, immutableList("PENDING", "PENDING_DELETE")));
        }

        public BoolBuilder must(Expression expr) { if (expr != null) { this.musts.add(expr); } return this; }

        public BoolBuilder musts(Expression... exprs) { for (Expression expr : exprs) { must(expr); } return this; }

        public BoolBuilder filter(BoolBuilder builder) { return filter(builder.build()); }

        public BoolBuilder filter(Expression expr) { if (expr != null) { this.filters.add(expr); } return this; }

        public BoolBuilder filters(Expression... exprs) { for (Expression expr : exprs) { filter(expr); } return this; }

        public BoolBuilder should(BoolBuilder builder) { return should(builder.build()); }

        public BoolBuilder should(Expression expr) { if (expr != null) { this.shoulds.add(expr); } return this; }

        public BoolBuilder shoulds(Expression... exprs) { for (Expression expr : exprs) { should(expr); } return this; }

        public BoolBuilder not(Expression expr) { if (expr != null) { this.mustNots.add(expr); } return this; }

        public BoolBuilder nots(Expression... exprs) { for (Expression expr : exprs) { not(expr); } return this; }

        public Expression build() {
            return withMap("Bool", "Bool", immutableResult("must", musts, "filter", filters, "should", shoulds, "must_not", mustNots));
        }
    }
}