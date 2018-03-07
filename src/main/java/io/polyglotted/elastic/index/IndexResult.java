package io.polyglotted.elastic.index;

import lombok.RequiredArgsConstructor;
import org.apache.http.HttpStatus;

@RequiredArgsConstructor
final class IndexResult {
    final int status;
    final String response;

    boolean isOk() { return HttpStatus.SC_OK == status; }
}