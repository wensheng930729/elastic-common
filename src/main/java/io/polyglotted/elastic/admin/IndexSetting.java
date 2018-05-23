package io.polyglotted.elastic.admin;

import io.polyglotted.common.model.Jsoner;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.model.MapResult.ImmutableResult;
import io.polyglotted.common.model.SortedMapResult;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import java.util.Map;

import static io.polyglotted.common.model.MapResult.immutableResult;
import static io.polyglotted.common.model.SortedMapResult.treeResult;
import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.common.util.BaseSerializer.serialize;
import static io.polyglotted.common.util.CollUtil.filterKeysNeg;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.common.util.ResourceUtil.readResource;

@SuppressWarnings({"unused", "WeakerAccess"})
@RequiredArgsConstructor @EqualsAndHashCode
public final class IndexSetting implements Jsoner {
    private static final String DEF_ANALYSIS = readResource(IndexSetting.class, "def-analysis.json");
    public final ImmutableResult mapResult;

    @Override public String toJson() { return serialize(filterKeysNeg(mapResult, "index_name"::equals)); }

    public static IndexSetting settingFrom(Map<String, Object> map) { return new IndexSetting(immutableResult(map)); }

    public static IndexSetting with(int numberOfShards, int numberOfReplicas) { return settingBuilder(numberOfShards, numberOfReplicas).build(); }

    public static IndexSetting autoReplicate() { return autoReplicate(DEF_ANALYSIS); }

    public static IndexSetting autoReplicate(String analysis) {
        return settingBuilder().numberOfShards(1).autoExpandReplicas().analysis(analysis).ignoreMalformed().refreshInterval(-1).build();
    }

    public static Builder settingBuilder(int numShards, int numReplicas) { return settingBuilder(numShards, numReplicas, DEF_ANALYSIS); }

    public static Builder settingBuilder(int numShards, int numReplicas, String analysis) {
        return settingBuilder().analysis(nonNull(analysis, DEF_ANALYSIS)).numberOfShards(numShards)
            .numberOfReplicas(numReplicas).refreshInterval(-1).ignoreMalformed();
    }

    public static Builder settingBuilder() { return new Builder(); }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private final SortedMapResult treeResult = treeResult();

        public Builder numberOfShards(int numberOfShards) { return any("number_of_shards", numberOfShards); }

        public Builder numberOfReplicas(int numberOfReplicas) { return any("number_of_replicas", numberOfReplicas); }

        public Builder refreshInterval(long refreshInterval) { return any("refresh_interval", refreshInterval); }

        public Builder ignoreMalformed() { treeResult.putIfAbsent("mapping.ignore_malformed", true); return this; }

        public Builder autoExpandReplicas() { treeResult.putIfAbsent("auto_expand_replicas", "0-all"); return this; }

        public Builder totalFields(Integer totalFieldsCount) { return any("mapping.total_fields.limit", totalFieldsCount); }

        public Builder nestedFields(Integer nestedFieldsCount) { return any("mapping.nested_fields.limit", nestedFieldsCount); }

        public Builder any(String name, Object value) { treeResult.put(name, value); return this; }

        public Builder all(MapResult result) { treeResult.putAll(result); return this; }

        public Builder analysis(String analysis) { return any("analysis", deserialize(analysis)); }

        public IndexSetting build() { return new IndexSetting(treeResult.immutable()); }
    }
}