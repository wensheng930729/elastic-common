package io.polyglotted.elastic.common;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public enum DocStatus {
    LIVE, UPDATED, DELETED, PENDING, PENDING_DELETE, REJECTED, DISCARDED;

    private static final Map<String, DocStatus> STATUS_MAP = buildStatusMap();

    public static DocStatus fromStatus(String status) { return STATUS_MAP.get(status); }

    private static Map<String, DocStatus> buildStatusMap() {
        ImmutableMap.Builder<String, DocStatus> builder = ImmutableMap.builder();
        for (DocStatus status : values()) {
            builder.put(status.name(), status);
            builder.put(status.name().toLowerCase(), status);
        }
        return builder.build();
    }

    @Override public String toString() { return name().toLowerCase(); }
}