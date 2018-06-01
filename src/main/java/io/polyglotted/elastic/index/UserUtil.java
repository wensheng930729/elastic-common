package io.polyglotted.elastic.index;

import io.polyglotted.common.model.AuthHeader;
import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.index.Validator.StrictValidator;
import io.polyglotted.elastic.search.Searcher;
import lombok.SneakyThrows;

import static io.polyglotted.common.model.MapResult.simpleResult;
import static io.polyglotted.common.util.BaseSerializer.serialize;
import static io.polyglotted.common.util.BaseSerializer.serializeBytes;
import static io.polyglotted.common.util.MapBuilder.immutableMap;
import static io.polyglotted.common.util.TokenUtil.uniqueToken;
import static io.polyglotted.elastic.client.XPackApi.PASSWD;
import static io.polyglotted.elastic.client.XPackApi.USER;
import static io.polyglotted.elastic.common.DocResult.filtered;
import static io.polyglotted.elastic.common.MetaFields.RESULT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.readKey;
import static io.polyglotted.elastic.common.MetaFields.timestamp;
import static io.polyglotted.elastic.common.Verbose.META;
import static io.polyglotted.elastic.index.IndexRecord.createRecord;
import static io.polyglotted.elastic.index.IndexRecord.updateRecord;
import static io.polyglotted.elastic.search.ResultBuilder.SourceBuilder;
import static org.apache.commons.codec.binary.Base64.encodeBase64;

@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class UserUtil {

    @SneakyThrows
    public static String writeUser(Indexer indexer, Searcher searcher, AuthHeader esAuth, String userRepo, String userModel,
                                   String userId, MapResult user, String subject, long millis) {
        user.remove("password");
        MapResult newValue = simpleResult("body", new String(encodeBase64(serializeBytes(user)), "utf-8"), "id", userId);
        MapResult existing = findMeta(searcher, esAuth, userRepo, userModel, userId);
        if (existing != null && newValue.equals(filtered(existing))) { return serialize(readKey(existing).put(RESULT_FIELD, "noop").build()); }

        return indexMeta(indexer, existing, userRepo, userModel, userId, newValue, esAuth, subject, millis, new StrictValidator() {
            @Override protected void preValidate(ElasticClient client, IndexRecord record) {
                if (existing == null) { user.put("password", uniqueToken()); }
                client.xpackPut(USER, userId, serialize(user));
            }
        });
    }

    public static void putPasswd(ElasticClient client, String userId, String password) {
        client.xpackPut(PASSWD, userId, serialize(immutableMap("password", password)));
    }

    private static String indexMeta(Indexer indexer, Object existing, String repo, String model, String id, Object newValue,
                                    AuthHeader esAuth, String userId, long millis, Validator validator) {
        IndexRecord.Builder record = ((existing == null) ? createRecord(repo, model, id, newValue) :
            updateRecord(repo, model, id, timestamp(existing), newValue)).user(userId).timestamp(millis);
        return indexer.strictSave(esAuth, record.build(), validator);
    }

    private static MapResult findMeta(Searcher searcher, AuthHeader esAuth, String repo, String model, String id) {
        return searcher.getById(esAuth, repo, model, id, SourceBuilder, META);
    }
}