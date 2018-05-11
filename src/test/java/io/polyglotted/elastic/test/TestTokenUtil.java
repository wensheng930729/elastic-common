/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.polyglotted.elastic.test;

import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.Signature;

import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.codec.binary.Base64.encodeBase64String;

abstract class TestTokenUtil {

    static String testToken() { return testToken(currentTimeMillis(), currentTimeMillis() + (24 * 60 * 60 * 1000)); }

    private static String testToken(long iat, long exp) {
        String contentBytes = encodeBase64String(header().getBytes(UTF_8)) + "." +
            encodeBase64String(payload((int) (iat / 1000), (int) (exp / 1000)).getBytes(UTF_8));
        return contentBytes + "." + encodeBase64String(sign(contentBytes));
    }

    private static byte[] sign(String contentBytes) {
        try {
            KeyPairGenerator rsa = KeyPairGenerator.getInstance("RSA");
            rsa.initialize(1024, SecureRandom.getInstance("SHA1PRNG"));
            Signature s = Signature.getInstance("SHA256withRSA");
            s.initSign(rsa.generateKeyPair().getPrivate());
            s.update(contentBytes.getBytes(UTF_8));
            return s.sign();
        } catch (Exception ex) { throw new RuntimeException("failed to sign", ex); }
    }

    private static String header() { return "{\"kid\":\"5eb0lDu5SNzmI5Rz6XFiBWE5bM\",\"alg\":\"RS256\"}"; }

    private static String payload(int issuedAt, int expiry) {
        return "{\"ver\":1,\"jti\":\"TuhepSxBJhODPWD1YT1KieQ\",\"iss\":\"https://dummy.dummy.com/oauth2/default\",\"aud\":\"api://default\"," +
            "\"iat\":" + issuedAt + ",\"exp\":" + expiry + ",\"uid\":\"abcdef1234ghij\",\"scp\":[\"openid\",\"profile\",\"email\"]," +
            "\"cognito:groups\":[\"ADMIN\",\"WRITER\",\"READER\"],\"sub\":\"tester@test.com\"}";
    }
}