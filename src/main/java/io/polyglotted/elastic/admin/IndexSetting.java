package io.polyglotted.elastic.admin;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.common.model.MapResult.ImmutableResult;
import io.polyglotted.common.model.SortedMapResult;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import static io.polyglotted.common.model.SortedMapResult.treeResult;
import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.common.util.BaseSerializer.serialize;
import static io.polyglotted.common.util.CollUtil.filterKeysNeg;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static io.polyglotted.common.util.ResourceUtil.readResource;

@SuppressWarnings({"unused", "WeakerAccess"})
@RequiredArgsConstructor @EqualsAndHashCode
public final class IndexSetting {
    private static final String DEF_ANALYSIS = readResource(IndexSetting.class, "def-analysis.json");
    public final ImmutableResult mapResult;

    public String createJson() { return serialize(filterKeysNeg(mapResult, "index_name"::equals)); }

    public static IndexSetting with(int numberOfShards, int numberOfReplicas) { return settingBuilder(numberOfShards, numberOfReplicas).build(); }

    public static IndexSetting autoReplicate() {
        return settingBuilder().numberOfShards(1).autoExpandReplicas().analysis(DEF_ANALYSIS).ignoreMalformed().disableDynamicMapping().build();
    }

    public static Builder settingBuilder(int numShards, int numReplicas) { return settingBuilder(numShards, numReplicas, DEF_ANALYSIS); }

    public static Builder settingBuilder(int numShards, int numReplicas, String analysis) {
        return settingBuilder().analysis(nonNull(analysis, DEF_ANALYSIS)).numberOfShards(numShards).numberOfReplicas(numReplicas).ignoreMalformed();
    }

    public static Builder settingBuilder() { return new Builder(); }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private final SortedMapResult treeResult = treeResult();

        public Builder numberOfShards(int numberOfShards) { treeResult.put("number_of_shards", numberOfShards); return this; }

        public Builder numberOfReplicas(int numberOfReplicas) { treeResult.put("number_of_replicas", numberOfReplicas); return this; }

        public Builder refreshInterval(long refreshInterval) { treeResult.put("refresh_interval", refreshInterval); return this; }

        public Builder ignoreMalformed() { treeResult.putIfAbsent("mapping.ignore_malformed", true); return this; }

        public Builder disableDynamicMapping() { treeResult.putIfAbsent("mapper.dynamic", false); return this; }

        public Builder autoExpandReplicas() { treeResult.putIfAbsent("auto_expand_replicas", "0-all"); return this; }

        public Builder any(String name, Object value) { treeResult.put(name, value); return this; }

        public Builder all(MapResult result) { treeResult.putAll(result); return this; }

        public Builder analysis(String analysis) { treeResult.put("analysis", deserialize(analysis)); return this; }

        public IndexSetting build() { return new IndexSetting(treeResult.immutable()); }
    }
}