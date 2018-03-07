package io.polyglotted.elastic.index;

import io.polyglotted.common.model.Pair;
import io.polyglotted.elastic.common.Notification;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import static io.polyglotted.common.model.Pair.pair;
import static io.polyglotted.common.util.UrnUtil.safeUrnOf;
import static io.polyglotted.elastic.common.MetaFields.ID_FIELD;
import static io.polyglotted.elastic.common.MetaFields.MODEL_FIELD;
import static io.polyglotted.elastic.common.MetaFields.RESULT_FIELD;
import static io.polyglotted.elastic.common.MetaFields.TIMESTAMP_FIELD;
import static io.polyglotted.elastic.index.ServerException.serverException;

@Slf4j abstract class IndexUtil {

    @SneakyThrows static void keyOf(BulkItemResponse response, long timestamp, XContentBuilder builder) {
        builder.field(MODEL_FIELD, response.getType());
        builder.field(ID_FIELD, response.getId());
        builder.field(TIMESTAMP_FIELD, timestamp);
    }

    @SneakyThrows static void keyOf(DocWriteResponse response, long timestamp, XContentBuilder builder) {
        builder.field(MODEL_FIELD, response.getType());
        builder.field(ID_FIELD, response.getId());
        builder.field(TIMESTAMP_FIELD, timestamp);
        builder.field(RESULT_FIELD, response.getResult().getLowercase());
    }

    static String keyOf(BulkItemResponse response, long ts) { return safeUrnOf(response.getType(), response.getId(), String.valueOf(ts)); }

    @SneakyThrows static Pair<Boolean, String> checkResponse(BulkResponse responses, IgnoreErrors ignore,
                                                             long timestamp, Notification.Builder notification) {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        boolean errorsFound = false;
        for (BulkItemResponse response : responses) {
            builder.field(response.getId());
            keyOf(response, timestamp, builder.startObject());
            if (response.isFailed()) {
                String failureMessage = response.getFailureMessage();
                if (!ignore.ignoreFailure(failureMessage)) {
                    errorsFound = true;
                    builder.field("failure", failureMessage);
                }
            }
            else {
                String actionResult = response.getResponse().getResult().getLowercase();
                builder.field(RESULT_FIELD, actionResult);
                if (notification != null && isChangeAction(actionResult)) {
                    notification.keyAction(response.getId(), keyOf(response, timestamp), actionResult);
                }
            }
            builder.endObject();
        }
        return pair(errorsFound, builder.endObject().string());
    }

    static RuntimeException logError(RuntimeException ex) {
        if (ex instanceof NoopException) { return ex; }
        else if (ex instanceof IndexerException) { log.error("two phase commit failed: " + ex.getMessage()); return ex; }
        else { log.error("two phase commit failed: " + ex.getMessage(), ex); return new IndexerException(ex.getMessage(), ex); }
    }

    static void failOnIndex(IndexResult result) {
        if (!result.isOk()) { log.error(result.response); throw serverException("internal error, failed to index"); }
    }

    @SneakyThrows static void logUnknown(Exception ex) { log.error("bulk index failed: " + ex.getMessage(), ex); throw ex; }

    private static boolean isChangeAction(String action) { return "created".equals(action) || "updated".equals(action) || "deleted".equals(action); }
}