package io.polyglotted.elastic.common;

import lombok.RequiredArgsConstructor;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

import javax.annotation.Nullable;

import static io.polyglotted.common.util.StrUtil.nullOrEmpty;
import static io.polyglotted.elastic.common.EsAuth.AuthType.BASIC;
import static io.polyglotted.elastic.common.EsAuth.AuthType.BEARER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.codec.binary.Base64.encodeBase64String;
import static org.apache.http.HttpHeaders.AUTHORIZATION;

@RequiredArgsConstructor
public class EsAuth {
    @Nullable public final String authHeader;

    public static EsAuth basicAuth(String user, String passwd) { return new EsAuth(BASIC.header(user, passwd)); }

    public static EsAuth bearerToken(String token) { return new EsAuth(BEARER.header(null, token)); }

    @SuppressWarnings("unused") public static EsAuth authHeader(Object auth) { return new EsAuth(auth == null ? null : String.valueOf(auth)); }

    public Header[] headers() { return nullOrEmpty(authHeader) ? new Header[0] : new Header[]{new BasicHeader(AUTHORIZATION, authHeader)}; }

    public enum AuthType {
        BASIC {
            @Override String header(String user, String creds) { return "Basic " + encodeBase64String((user + ":" + creds).getBytes(UTF_8)); }
        },
        BEARER {
            @Override String header(String user, String token) { return "Bearer " + token; }
        };

        abstract String header(String user, String creds);
    }
}