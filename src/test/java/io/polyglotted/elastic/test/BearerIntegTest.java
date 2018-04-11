package io.polyglotted.elastic.test;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.EsAuth;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static io.polyglotted.common.util.ResourceUtil.readResourceAsMap;
import static io.polyglotted.elastic.client.ElasticSettings.elasticSettings;
import static io.polyglotted.elastic.client.HighLevelConnector.highLevelClient;
import static io.polyglotted.elastic.client.XPackApi.ROLE;
import static io.polyglotted.elastic.client.XPackApi.TOKEN;
import static io.polyglotted.elastic.client.XPackApi.USER;
import static io.polyglotted.elastic.common.EsAuth.basicAuth;
import static io.polyglotted.elastic.common.EsAuth.bearerToken;
import static io.polyglotted.elastic.test.TestTokenUtil.testToken;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class BearerIntegTest {
    private static final Map<String, String> MESSAGES = readResourceAsMap(BearerIntegTest.class, "roles-users.txt");
    private static final EsAuth SUPER_USER = basicAuth("elastic", "SteelEye");
    private ElasticClient client;

    @Before public void init() {
        client = highLevelClient(elasticSettings());
        client.xpackPut(SUPER_USER, ROLE, "jwt_user", MESSAGES.get("jwtUser.role"));
    }

    @After public void close() {
        client.xpackDelete(SUPER_USER, ROLE, "jwt_user");
        client.close();
    }

    @Test
    public void testBearerAuth() throws Exception {
        assertThat(client.clusterHealth(bearerToken(testToken())), is(notNullValue()));

        client.xpackPut(SUPER_USER, USER, "jacknich", MESSAGES.get("user.put"));
        MapResult result = client.xpackPut(SUPER_USER, TOKEN, "", MESSAGES.get("token.post"));
        String accessToken = result.reqdStr("access_token");
        client.xpackDelete(SUPER_USER, TOKEN, "", "{\"token\":\"" + accessToken+ "\"}");
        client.xpackDelete(SUPER_USER, USER, "jacknich");
    }
}