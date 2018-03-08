package io.polyglotted.elastic.common;

import lombok.RequiredArgsConstructor;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.commons.codec.binary.Base64.encodeBase64String;
import static org.apache.http.HttpHeaders.AUTHORIZATION;

@RequiredArgsConstructor
public class EsAuth {
    public final String user;
    public final String creds;
    public final AuthType type;

    public Header header() { return new BasicHeader(AUTHORIZATION, type.header(this)); }

    public enum AuthType {
        BASIC {
            @Override String header(EsAuth auth) { return "Basic " + encodeBase64String((auth.user + ":" + auth.creds).getBytes(UTF_8)); }
        },
        BEARER {
            @Override String header(EsAuth auth) { return "Bearer " + auth.creds; }
        };

        abstract String header(EsAuth auth);
    }
}