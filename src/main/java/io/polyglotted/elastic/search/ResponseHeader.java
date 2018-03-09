package io.polyglotted.elastic.search;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Map;

import static io.polyglotted.common.util.ConversionUtil.asLong;
import static io.polyglotted.common.util.MapRetriever.optStr;
import static io.polyglotted.common.util.MapRetriever.optValue;

@SuppressWarnings({"unused", "WeakerAccess"})
@ToString(includeFieldNames = false, doNotUseGetters = true)
@EqualsAndHashCode @RequiredArgsConstructor
public final class ResponseHeader {
    public final long tookInMillis;
    public final long totalHits;
    public final long returnedHits;
    public final String scrollId;

    public static ResponseHeader deserializeHeader(Map<String, Object> map) {
        return new ResponseHeader(asLong(optValue(map, "tookInMillis", -1L)), asLong(optValue(map, "totalHits", -1L)),
            asLong(optValue(map, "returnedHits", -1L)), optStr(map, "scrollId"));
    }
}