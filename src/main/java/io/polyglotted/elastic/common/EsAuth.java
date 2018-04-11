package io.polyglotted.elastic.common;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import javax.annotation.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.codec.binary.Base64.encodeBase64String;
import static org.apache.http.HttpHeaders.AUTHORIZATION;

@RequiredArgsConstructor
public class EsAuth {
    @Nullable public final String user;
    @NonNull public final String creds;
    @NonNull public final AuthType type;

    public static EsAuth basicAuth(String user, String passwd) { return new EsAuth(user, passwd, AuthType.BASIC); }

    public static EsAuth bearerToken(String token) { return new EsAuth(null, token, AuthType.BEARER); }

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