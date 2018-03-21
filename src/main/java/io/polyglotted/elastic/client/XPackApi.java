package io.polyglotted.elastic.client;

import io.polyglotted.common.util.HttpRequestBuilder.HttpReqType;
import lombok.RequiredArgsConstructor;

import static io.polyglotted.common.util.HttpRequestBuilder.HttpReqType.PUT;
import static lombok.AccessLevel.PRIVATE;

@RequiredArgsConstructor(access = PRIVATE)
public enum XPackApi {
    ROLE(PUT, "/_xpack/security/role/"),
    USER(PUT, "/_xpack/security/user/");

    final HttpReqType type;
    final String endpoint;
}