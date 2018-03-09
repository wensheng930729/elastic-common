package io.polyglotted.elastic.search;

import com.google.common.collect.ImmutableList;
import io.polyglotted.common.model.MapResult;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.polyglotted.common.model.MapResult.immutableResult;
import static io.polyglotted.common.util.BaseSerializer.serialize;

@RequiredArgsConstructor
public final class Expression {
    static final Expression NilExpression = new Expression("_nil_", "_nil_", immutableResult());
    static final String ValueKey = "_val";
    public final String operation;
    public final String label;
    public final MapResult args;

    static Expression withMap(String operation, String label, Map<String, Object> args) {
        return new Expression(checkNotNull(operation), checkNotNull(label), immutableResult(args));
    }

    static Expression withValue(String expressionType, String label, Object valueArg) {
        return new Expression(expressionType, checkNotNull(label), immutableResult(ValueKey, valueArg));
    }

    static <E extends Comparable<E>> Expression withArray(String expressionType, String label, List<E> valueArg) {
        return new Expression(expressionType, checkNotNull(label), immutableResult(ValueKey, valueArg));
    }

    static Expression withLabel(String expressionType, String label) {
        return new Expression(expressionType, checkNotNull(label), immutableResult());
    }

    <T> T valueArg() { return argFor(ValueKey, null); }

    List<Object> arrayArg() { return argFor(ValueKey, ImmutableList.of()); }

    String stringArg(String key) { return argFor(key, null); }

    <T> T argFor(String key) { return checkNotNull(argFor(key, null)); }

    @SuppressWarnings("unchecked") private <T> T argFor(String key, T defValue) { return args.containsKey(key) ? (T) args.get(key) : defValue; }

    @Override public boolean equals(Object o) { return this == o || o != null && getClass() == o.getClass() && serialize(this).equals(serialize(o)); }

    @Override public int hashCode() { return Objects.hash(operation, label, args); }

    @Override public String toString() { return (isNullOrEmpty(label) ? "" : label + " ") + operation + (args.isEmpty() ? "" : " " + args); }
}