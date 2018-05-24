package io.polyglotted.elastic.client;

import io.polyglotted.common.util.HttpRequestBuilder.HttpReqType;
import lombok.RequiredArgsConstructor;

import static io.polyglotted.common.util.HttpRequestBuilder.HttpReqType.POST;
import static io.polyglotted.common.util.HttpRequestBuilder.HttpReqType.PUT;
import static io.polyglotted.common.util.NullUtil.nonNull;
import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access = PRIVATE)
public enum XPackApi {
    TOKEN(POST, "/_xpack/security/oauth2/token"),
    ROLE(PUT, "/_xpack/security/role/{id}"),
    USER(PUT, "/_xpack/security/user/{id}"),
    PASSWD(POST, "/_xpack/security/user/{id}/_password");

    final HttpReqType type;
    private final String endpoint;

    String endpointWith(String id) { return endpoint.replace("{id}", nonNull(id, "")); }
}