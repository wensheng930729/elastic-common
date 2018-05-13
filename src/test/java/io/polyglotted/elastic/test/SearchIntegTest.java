package io.polyglotted.elastic.test;

import io.polyglotted.common.model.MapResult;
import io.polyglotted.elastic.client.ElasticClient;
import io.polyglotted.elastic.common.Verbose;
import io.polyglotted.elastic.index.BulkRecord;
import io.polyglotted.elastic.index.Indexer;
import io.polyglotted.elastic.search.Searcher;
import io.polyglotted.elastic.search.QueryResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.polyglotted.common.model.MapResult.immutableResult;
import static io.polyglotted.common.model.MapResult.simpleResultBuilder;
import static io.polyglotted.common.util.BaseSerializer.deserialize;
import static io.polyglotted.common.util.BaseSerializer.serialize;
import static io.polyglotted.common.util.ListBuilder.immutableList;
import static io.polyglotted.common.util.MapRetriever.deepRetrieve;
import static io.polyglotted.common.util.ResourceUtil.readResourceAsMap;
import static io.polyglotted.common.util.TokenUtil.uniqueToken;
import static io.polyglotted.elastic.common.Verbose.ID;
import static io.polyglotted.elastic.common.Verbose.NONE;
import static io.polyglotted.elastic.index.BulkRecord.bulkBuilder;
import static io.polyglotted.elastic.search.QueryMaker.DEFAULT_KEEP_ALIVE;
import static io.polyglotted.elastic.search.QueryMaker.copyFrom;
import static io.polyglotted.elastic.search.QueryMaker.filterToScroller;
import static io.polyglotted.elastic.search.ResultBuilder.NullBuilder;
import static io.polyglotted.elastic.search.ResultBuilder.SourceBuilder;
import static io.polyglotted.elastic.test.ElasticTestUtil.ES_AUTH;
import static io.polyglotted.elastic.test.ElasticTestUtil.testElasticClient;
import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SearchIntegTest {
    private static final Map<String, String> MESSAGES = readResourceAsMap(SearchIntegTest.class, "search-integ.txt");
    private ElasticClient client;
    private Indexer indexer;
    private Searcher searcher;

    @Before public void init() {
        client = testElasticClient(); indexer = new Indexer(client); searcher = new Searcher(client);
    }

    @After public void close() { client.close(); }

    @Test public void searchSuccess() throws Exception {
        String agex = client.createIndex(new CreateIndexRequest(uniqueToken()).source(MESSAGES.get("agex.source"), JSON));
        try {
            BulkRecord bulkRecord = bulkBuilder("agex", "Trade", currentTimeMillis(), "tester").objects(buildTradesJson()).build();
            boolean result = indexer.bulkSave(ES_AUTH, bulkRecord);
            assertThat(result, is(true)); assertThat(serialize(bulkRecord.failures), serialize(bulkRecord.failures), is("{}"));

            int expectedHits;
            for (int i = 1; i <= 14; i++) {
                expectedHits = (i == 13) ? 1 : 16;
                simpleAgg(MESSAGES.get("agg" + i + ".query"), expectedHits, MESSAGES.get("agg" + i + ".result"));
                flattenAgg(MESSAGES.get("agg" + i + ".query"), expectedHits, MESSAGES.get("agg" + i + ".flat"));
            }
            simpleSearchAndScroll();

        } finally { client.dropIndex(agex); }
    }

    private void simpleAgg(String query, int hits, String result) throws Exception {
        String aggregations = searchNative(query, false, NONE, hits, "aggregations");
        assertThat(aggregations, aggregations, is(result));
    }

    private void flattenAgg(String query, int hits, String result) throws Exception {
        String flattened = searchNative(query, true, NONE, hits, "flattened");
        assertThat(flattened, flattened, is(result));
    }

    private void simpleSearchAndScroll() throws Exception {
        String textResponse = searchNative(MESSAGES.get("text.query"), false, ID, 5, "results");
        assertThat(textResponse, textResponse, is(MESSAGES.get("text.response")));

        int totalHits = 0;
        QueryResponse response = searcher.searchBy(ES_AUTH, filterToScroller("agex", null, 8), NullBuilder, NONE);
        long returnedHits = response.header.returnedHits;
        while (response.hasNextScroll()) {
            totalHits += returnedHits;
            response = searcher.scroll(ES_AUTH, response.scrollId(), DEFAULT_KEEP_ALIVE, NullBuilder, NONE);
            returnedHits = response.header.returnedHits;
        }
        assertThat(totalHits, is(16));
    }

    private String searchNative(String query, boolean flatten, Verbose verb, int totalHits, String resultKey) throws IOException {
        MapResult mapResult = deserialize(searcher.searchNative(ES_AUTH, copyFrom("agex",
            query.getBytes(UTF_8), null, verb), SourceBuilder, flatten, verb));
        assertThat(deepRetrieve(mapResult, "header.totalHits"), is(totalHits));
        return serialize(mapResult.get(resultKey));
    }

    private static List<MapResult> buildTradesJson() {
        List<MapResult> trades = new ArrayList<>(20);
        trades.add(trade("trades:001", "EMEA", "UK", "London", "IEU", "Alex", zdt(1425427200000L), 20.0));
        trades.add(trade("trades:002", "EMEA", "UK", "London", "IEU", "Andrew", zdt(1420848000000L), 15.0,
            leg("leg:002:001", "a", 10, 12.5), leg("leg:002:002", "b", 3, 2.5)));
        trades.add(trade("trades:003", "EMEA", "UK", "London", "IEU", "Bob", zdt(1425427200000L), 12.0,
            leg("leg:003:001", "c", 6, 12)));
        trades.add(trade("trades:004", "EMEA", "UK", "London", "NYM", "Charlie", zdt(1423958400000L), 25.0,
            leg("leg:004:001", "d", 5, 15), leg("leg:004:002", "a", 2, 5), leg("leg:004:003", "b", 3, 5)));
        trades.add(trade("trades:005", "EMEA", "UK", "London", "LME", "Chandler", zdt(1422144000000L), 20.0));
        trades.add(trade("trades:006", "EMEA", "UK", "London", "LME", "Duncan", zdt(1420848000000L), 10.0,
            leg("leg:006:001", "c", 2, 10)));
        trades.add(trade("trades:007", "EMEA", "UK", "London", "LME", "David", zdt(1423958400000L), 30.0,
            leg("leg:007:001", "d", 10, 20.5), leg("leg:007:002", "a", 5, 9.5)));
        trades.add(trade("trades:008", "EMEA", "CH", "Geneva", "IEU", "Ellott", zdt(1422144000000L), 20.0,
            leg("leg:008:001", "b", 6, 6), leg("leg:008:002", "c", 6, 6), leg("leg:008:003", "d", 4, 4)));
        trades.add(trade("trades:009", "EMEA", "CH", "Geneva", "NYM", "Fred", zdt(1425427200000L), 16.0));
        trades.add(trade("trades:010", "EMEA", "CH", "Zurich", "NYM", "Gabriel", zdt(1423958400000L), 32.0,
            leg("leg:010:001", "a", 8, 32)));
        trades.add(trade("trades:011", "EMEA", "CH", "Zurich", "IUS", "Pier", zdt(1422144000000L), 11.0,
            leg("leg:011:001", "b", 5, 8.3), leg("leg:011:002", "c", 1, 2.7)));
        trades.add(trade("trades:012", "NA", "US", "Stamford", "IUS", "Longfellow", zdt(1420848000000L), 20.0,
            leg("leg:012:001", "d", 5, 12.5), leg("leg:012:002", "a", 2, 4.25), leg("leg:012:003", "b", 2, 3.25)));
        trades.add(trade("trades:013", "NA", "US", "Stamford", "IUS", "Rose", zdt(1425427200000L), 50.0));
        trades.add(trade("trades:014", "NA", "US", "Stamford", "NYM", "Longfellow", zdt(1423958400000L), 10.0,
            leg("leg:014:001", "c", 3, 10)));
        trades.add(trade("trades:015", "NA", "US", "Houston", "IUS", "Shaun", zdt(1425427200000L), 50.0,
            leg("leg:015:001", "d", 20, 30.75), leg("leg:015:002", "a", 10, 19.25)));
        trades.add(trade("trades:016", "NA", "US", "Houston", "NYM", "Alex", zdt(1425427200000L), 40.0,
            leg("leg:016:001", "b", 10, 20), leg("leg:016:002", "c", 5, 10), leg("leg:016:003", "d", 5, 10)));
        return trades;
    }

    private static MapResult leg(String legnum, String product, int qty, double amount) {
        return immutableResult("legnum", legnum, "product", product, "qty", qty, "amount", amount);
    }

    private static String zdt(Long lval) { return ZonedDateTime.ofInstant(Instant.ofEpochMilli(lval), ZoneOffset.UTC).toString(); }

    private static MapResult trade(String tradeId, String region, String country, String city, String exchange,
                                   String trader, Object date, double value, MapResult... legs) {
        return simpleResultBuilder().put("tradeId", tradeId).put("region", region).put("country", country)
            .put("city", city).put("exchange", exchange).put("trader", trader).put("date", String.valueOf(date)).put("value", value)
            .putList("legs", immutableList(legs)).result();
    }
}