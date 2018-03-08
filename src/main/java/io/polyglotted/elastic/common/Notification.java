package io.polyglotted.elastic.common;

import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static io.polyglotted.common.util.StrUtil.safePrefix;
import static io.polyglotted.common.util.StrUtil.safeSuffix;

@SuppressWarnings({"unused", "WeakerAccess", "UnusedReturnValue"})
@RequiredArgsConstructor @EqualsAndHashCode
public class Notification {
    public final String realm;
    public final ImmutableList<KeyAction> keyActions;

    public static Notification notification(String realm, String id, String key, String singleResult) {
        return new Notification(realm, ImmutableList.of(new KeyAction(id, key, safePrefix(safeSuffix(singleResult, "&result\":\""), "\""))));
    }

    public static Builder notificationBuilder() { return new Builder(); }

    @Accessors(fluent = true) @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class KeyAction {
        public final String id;
        @Getter public final String key;
        public final String action;

        KeyAction() { this(null, null, null); }
    }

    @Setter
    @Accessors(fluent = true, chain = true)
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private String realm;
        private final Map<String, String> simpleKeys = new LinkedHashMap<>();
        private final List<KeyAction> keyActions = new LinkedList<>();

        public Builder key(String simpleKey, String key) { simpleKeys.put(simpleKey, key); return this; }

        public Builder keyAction(String id, String key, String action) {
            if (simpleKeys.containsKey(key)) { keyActions.add(new KeyAction(id, simpleKeys.get(key), action)); } return this;
        }

        public Notification build() { return new Notification(realm, ImmutableList.copyOf(keyActions)); }
    }
}