package com.redis.lettucemod;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.reactive.RedisBloomReactiveCommands;
import com.redis.lettucemod.api.sync.RedisBloomCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.api.sync.RedisTimeSeriesCommands;
import com.redis.lettucemod.bloom.*;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.search.*;
import com.redis.lettucemod.timeseries.*;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.*;
import io.lettuce.core.search.arguments.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startable;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

import static com.redis.lettucemod.Beers.*;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@TestInstance(Lifecycle.PER_CLASS)
abstract class ModulesTests {

    protected static final String LABEL_SENSOR_ID = "sensor_id";

    protected static final String LABEL_AREA_ID = "area_id";

    protected static final long TIMESTAMP_1 = 1548149181;

    protected static final long TIMESTAMP_2 = 1548149191;

    protected static final double VALUE_1 = 30;

    protected static final double VALUE_2 = 42;

    protected static final String TS_KEY = "temperature:3:11";

    protected static final String TS_KEY_2 = "temperature:3:12";

    protected static final String SENSOR_ID = "2";

    protected static final String AREA_ID = "32";

    protected static final String AREA_ID_2 = "34";

    protected static final String FILTER = LABEL_SENSOR_ID + "=" + SENSOR_ID;

    protected static final String SUGINDEX = "beersSug";

    private static final String PONG = "PONG";

    protected static Map<String, String> mapOf(String... keyValues) {
        Map<String, String> map = new HashMap<>();
        for (int index = 0; index < keyValues.length / 2; index++) {
            map.put(keyValues[index * 2], keyValues[index * 2 + 1]);
        }
        return map;
    }

    private AbstractRedisClient client;

    protected StatefulRedisModulesConnection<String, String> connection;

    private StatefulRedisModulesConnection<String, String> asyncConnection;

    protected RedisModulesCommands<String, String> commands;

    @BeforeAll
    void setup() {
        RedisServer server = getRedisServer();
        if (server instanceof Startable) {
            ((Startable) server).start();
        }
        String uri = server.getRedisURI();
        if (server.isRedisCluster()) {
            RedisModulesClusterClient clusterClient = RedisModulesClusterClient.create(uri);
            client = clusterClient;
            connection = clusterClient.connect();
            asyncConnection = clusterClient.connect();
        } else {
            RedisModulesClient redisClient = RedisModulesClient.create(uri);
            client = redisClient;
            connection = redisClient.connect();
            asyncConnection = redisClient.connect();
        }
        commands = connection.sync();
    }

    @AfterAll
    void teardown() {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
            client.getResources().shutdown();
        }
        RedisServer server = getRedisServer();
        if (server instanceof Startable) {
            ((Startable) server).stop();
        }
    }

    @BeforeEach
    void setupRedis() throws InterruptedException {
        commands.flushall();
    }

    protected abstract RedisServer getRedisServer();

    protected void assertPing(StatefulRedisModulesConnection<String, String> connection) {
        assertEquals(PONG, ping(connection));
    }

    protected String ping(StatefulRedisModulesConnection<String, String> connection) {
        return commands.ping();
    }

    @Test
    void ftInfoInexistentIndex() {
        Assertions.assertThrows(RedisCommandExecutionException.class, () -> commands.ftInfo("sdfsdfs"), "Unknown Index name");
    }

    private int populateIndex() throws IOException {
        return Beers.populateIndex(asyncConnection);
    }

    @SuppressWarnings("unchecked")
    @Test
    void tsAdd() {
        RedisTimeSeriesCommands<String, String> ts = commands;
        // TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
        // TS.ADD temperature:3:11 1548149181 30
        Long add1 = ts.tsAdd(TS_KEY, Sample.of(TIMESTAMP_1, VALUE_1),
                AddOptions.<String, String> builder().retentionPeriod(6000)
                        .labels(KeyValue.just(LABEL_SENSOR_ID, SENSOR_ID), KeyValue.just(LABEL_AREA_ID, AREA_ID)).build());
        assertEquals(TIMESTAMP_1, add1);
        Sample sample = ts.tsGet(TS_KEY);
        assertEquals(TIMESTAMP_1, sample.getTimestamp());
        // TS.ADD temperature:3:11 1548149191 42
        Long add2 = ts.tsAdd(TS_KEY, Sample.of(TIMESTAMP_2, VALUE_2));
        assertEquals(TIMESTAMP_2, add2);
    }

    @Test
    void tsRange() {
        populate();
        assertRange(commands.tsRange(TS_KEY, TimeRange.builder().from(TIMESTAMP_1 - 10).to(TIMESTAMP_2 + 10).build(),
                RangeOptions.builder()
                        .aggregation(Aggregation.aggregator(Aggregator.AVG).bucketDuration(Duration.ofMillis(5)).build())
                        .build()));
        assertRange(commands.tsRange(TS_KEY, TimeRange.unbounded(), RangeOptions.builder()
                .aggregation(Aggregation.aggregator(Aggregator.AVG).bucketDuration(Duration.ofMillis(5)).build()).build()));
        assertRange(commands.tsRange(TS_KEY, TimeRange.from(TIMESTAMP_1 - 10).build(), RangeOptions.builder()
                .aggregation(Aggregation.aggregator(Aggregator.AVG).bucketDuration(Duration.ofMillis(5)).build()).build()));
        assertRange(commands.tsRange(TS_KEY, TimeRange.to(TIMESTAMP_2 + 10).build(), RangeOptions.builder()
                .aggregation(Aggregation.aggregator(Aggregator.AVG).bucketDuration(Duration.ofMillis(5)).build()).build()));
    }

    private void assertRange(List<Sample> results) {
        assertEquals(2, results.size());
        assertEquals(1548149180, results.get(0).getTimestamp());
        assertEquals(VALUE_1, results.get(0).getValue());
        assertEquals(1548149190, results.get(1).getTimestamp());
        assertEquals(VALUE_2, results.get(1).getValue());
    }

    @SuppressWarnings("unchecked")
    protected void populate() {
        // TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
        // TS.ADD temperature:3:11 1548149181 30
        commands.tsAdd(TS_KEY, Sample.of(TIMESTAMP_1, VALUE_1), AddOptions.<String, String> builder().retentionPeriod(6000)
                .labels(KeyValue.just(LABEL_SENSOR_ID, SENSOR_ID), KeyValue.just(LABEL_AREA_ID, AREA_ID)).build());
        // TS.ADD temperature:3:11 1548149191 42
        commands.tsAdd(TS_KEY, Sample.of(TIMESTAMP_2, VALUE_2));

        commands.tsAdd(TS_KEY_2, Sample.of(TIMESTAMP_1, VALUE_1), AddOptions.<String, String> builder().retentionPeriod(6000)
                .labels(KeyValue.just(LABEL_SENSOR_ID, SENSOR_ID), KeyValue.just(LABEL_AREA_ID, AREA_ID_2)).build());
        commands.tsAdd(TS_KEY_2, Sample.of(TIMESTAMP_2, VALUE_2));
    }

    @Test
    void tsGet() {
        RedisTimeSeriesCommands<String, String> ts = commands;
        populate();
        Sample result = ts.tsGet(TS_KEY);
        Assertions.assertEquals(TIMESTAMP_2, result.getTimestamp());
        Assertions.assertEquals(VALUE_2, result.getValue());
        ts.tsCreate("ts:empty", com.redis.lettucemod.timeseries.CreateOptions.<String, String> builder().build());
        Assertions.assertNull(ts.tsGet("ts:empty"));
    }

    protected void assertJSONEquals(String expected, String actual) throws JsonMappingException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        assertEquals(mapper.readTree(expected), mapper.readTree(actual));
    }

    @Test
    void bfBasic() {
        String key = "test:bfBasic";
        String keyInsert = "test:bfInsert";
        RedisBloomCommands<String, String> bf = commands;
        commands.unlink(key, keyInsert);
        String status = bf.bfReserve(key, .01, 1000);
        assertEquals("OK", status);
        Boolean added = bf.bfAdd(key, "test");
        assertTrue(added);
        assertTrue(bf.bfExists(key, "test"));
        List<Boolean> res = bf.bfMAdd(key, "test", "one", "two");
        assertTrue(!res.get(0) && res.get(1) && res.get(2));
        res = bf.bfMExists(key, "test", "one", "two", "three");
        assertTrue(res.get(0) && res.get(1) && res.get(2) && !res.get(3));
        assertEquals(3, bf.bfCard(key));
        BloomFilterInsertOptions options = BloomFilterInsertOptions.builder().capacity(10000).error(.01).expansion(2).build();
        List<Boolean> inserted = bf.bfInsert(keyInsert, options, "three", "four", "five");
        assertTrue(inserted.get(0) && inserted.get(1) && inserted.get(2));
        BloomFilterInfo info = bf.bfInfo(keyInsert);
        Long capacity = bf.bfInfo(keyInsert, BloomFilterInfoType.CAPACITY);
        assertEquals(10000, capacity);
        assertEquals(10000, info.getCapacity());
        assertEquals(2, info.getExpansionRate());
        assertEquals(3, info.getNumInserted());
    }

    @Test
    void bfReactive() {
        String key = "test:reactive:bfBasic";
        String keyInsert = "test:reactive:bfInsert";
        commands.unlink(key, keyInsert);

        RedisBloomReactiveCommands<String, String> bf = connection.reactive();

        Mono<String> status = bf.bfReserve(key, .01, 1000);
        assertEquals("OK", status.block());
        Boolean added = bf.bfAdd(key, "test").block();
        assertTrue(added);
        assertTrue(bf.bfExists(key, "test").block());
        List<Boolean> res = bf.bfMAdd(key, "test", "one", "two").collectList().block();
        assertTrue(!res.get(0) && res.get(1) && res.get(2));
        res = bf.bfMExists(key, "test", "one", "two", "three").collectList().block();
        assertTrue(res.get(0) && res.get(1) && res.get(2) && !res.get(3));
        assertEquals(3, bf.bfCard(key).block());
        BloomFilterInsertOptions options = BloomFilterInsertOptions.builder().capacity(10000).error(.01).expansion(2).build();
        List<Boolean> inserted = bf.bfInsert(keyInsert, options, "three", "four", "five").collectList().block();
        assertTrue(inserted.get(0) && inserted.get(1) && inserted.get(2));
        BloomFilterInfo info = bf.bfInfo(keyInsert).block();
        Long capacity = bf.bfInfo(keyInsert, BloomFilterInfoType.CAPACITY).block();
        assertEquals(10000, capacity);
        assertEquals(10000, info.getCapacity());
        assertEquals(2, info.getExpansionRate());
        assertEquals(3, info.getNumInserted());
    }

    @Test
    void cfBasic() {
        String key1 = "cf:test:key";
        String key2 = "cf:test:insert:key";
        commands.unlink(key1, key2);
        RedisBloomCommands<String, String> cf = commands;
        assertEquals("OK", cf.cfReserve(key1, 1000L));
        assertTrue(cf.cfAdd(key1, "test"));
        assertFalse(cf.cfAddNx(key1, "test"));
        assertEquals(1, cf.cfCount(key1, "test"));
        assertTrue(cf.cfDel(key1, "test"));
        CuckooFilterInsertOptions options = CuckooFilterInsertOptions.builder().capacity(10000L).noCreate(false).build();
        List<Long> insertResult = cf.cfInsert(key2, options, "test", "one", "two");
        assertEquals(1, insertResult.get(0));
        assertEquals(1, insertResult.get(1));
        assertEquals(1, insertResult.get(2));
        insertResult = cf.cfInsertNx(key2, "test", "one", "three");
        assertEquals(0, insertResult.get(0));
        assertEquals(0, insertResult.get(1));
        assertEquals(1, insertResult.get(2));
        CuckooFilter info = cf.cfInfo(key2);
        assertEquals(4, info.getNumItemsInserted());
        List<Boolean> exists = cf.cfMExists(key2, "test", "one", "two", "three");
        for (Boolean b : exists) {
            assertTrue(b);
        }
    }

    @Test
    void cfBasicReactive() {
        String key1 = "cf:reactive:test:key";
        String key2 = "cf:reactive:test:insert:key";
        commands.unlink(key1, key2);
        RedisBloomReactiveCommands<String, String> cf = connection.reactive();
        assertEquals("OK", cf.cfReserve(key1, 1000L).block());
        assertEquals(Boolean.TRUE, cf.cfAdd(key1, "test").block());
        assertNotEquals(Boolean.TRUE, cf.cfAddNx(key1, "test").block());
        assertEquals(1, cf.cfCount(key1, "test").block());
        assertEquals(Boolean.TRUE, cf.cfDel(key1, "test").block());
        CuckooFilterInsertOptions options = CuckooFilterInsertOptions.builder().capacity(10000L).noCreate(false).build();
        List<Long> insertResult = cf.cfInsert(key2, options, "test", "one", "two").collectList().block();
        assertNotNull(insertResult);
        assertEquals(1, insertResult.get(0));
        assertEquals(1, insertResult.get(1));
        assertEquals(1, insertResult.get(2));
        insertResult = cf.cfInsertNx(key2, "test", "one", "three").collectList().block();
        assertNotNull(insertResult);
        assertEquals(0, insertResult.get(0));
        assertEquals(0, insertResult.get(1));
        assertEquals(1, insertResult.get(2));
        CuckooFilter info = cf.cfInfo(key2).block();
        assertNotNull(info);
        assertEquals(4, info.getNumItemsInserted());
        List<Boolean> exists = cf.cfMExists(key2, "test", "one", "two", "three").collectList().block();
        assertNotNull(exists);
        for (Boolean b : exists) {
            assertTrue(b);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void cms() {
        String key1 = "{cms}:1";
        String key2 = "{cms}:2";
        String outKey = "{cms}:out";
        String key3 = "{cms}:3";

        commands.unlink(key1, key2);
        RedisBloomCommands<String, String> cms = commands;

        assertEquals("OK", cms.cmsInitByProb(key1, .001, .01));
        assertEquals(2, cms.cmsIncrBy(key1, "test", 2));
        List<Long> result = cms.cmsIncrBy(key1, LongScoredValue.just(1, "one"), LongScoredValue.just(2, "two"),
                LongScoredValue.just(3, "three"));
        assertEquals(1, result.get(0));
        assertEquals(2, result.get(1));
        assertEquals(3, result.get(2));

        result = cms.cmsQuery(key1, "one", "two", "three");
        assertEquals(1, result.get(0));
        assertEquals(2, result.get(1));
        assertEquals(3, result.get(2));

        CmsInfo info = cms.cmsInfo(key1);
        assertEquals(8, info.getCount());
        assertEquals(2000, info.getWidth());
        assertEquals(7, info.getDepth());

        assertEquals("OK", cms.cmsInitByDim(key2, 10000, 5));
        info = cms.cmsInfo(key2);
        assertEquals(0, info.getCount());
        assertEquals(10000, info.getWidth());
        assertEquals(5, info.getDepth());

        cms.cmsInitByProb(key3, .001, .01);
        cms.cmsInitByProb(outKey, .001, .01);
        assertEquals("OK", cms.cmsMerge(outKey, key1, key3));
    }

    @SuppressWarnings("unchecked")
    @Test
    void cmsReactive() {
        String key1 = "{cms}:1";
        String key2 = "{cms}:2";
        String outKey = "{cms}:out";
        String key3 = "{cms}:3";

        commands.unlink(key1, key2);
        RedisBloomReactiveCommands<String, String> cms = connection.reactive();

        assertEquals("OK", cms.cmsInitByProb(key1, .001, .01).block());
        assertEquals(2, cms.cmsIncrBy(key1, "test", 2).block());
        List<Long> result = cms.cmsIncrBy(key1, LongScoredValue.just(1, "one"), LongScoredValue.just(2, "two"),
                LongScoredValue.just(3, "three")).collectList().block();
        assertNotNull(result);
        assertEquals(1, result.get(0));
        assertEquals(2, result.get(1));
        assertEquals(3, result.get(2));

        result = cms.cmsQuery(key1, "one", "two", "three").collectList().block();
        assertNotNull(result);
        assertEquals(1, result.get(0));
        assertEquals(2, result.get(1));
        assertEquals(3, result.get(2));

        CmsInfo info = cms.cmsInfo(key1).block();
        assertNotNull(info);
        assertEquals(8, info.getCount());
        assertEquals(2000, info.getWidth());
        assertEquals(7, info.getDepth());

        assertEquals("OK", cms.cmsInitByDim(key2, 10000, 5).block());
        info = cms.cmsInfo(key2).block();
        assertNotNull(info);
        assertEquals(0, info.getCount());
        assertEquals(10000, info.getWidth());
        assertEquals(5, info.getDepth());

        cms.cmsInitByProb(key3, .001, .01).block();
        cms.cmsInitByProb(outKey, .001, .01).block();
        assertEquals("OK", cms.cmsMerge(outKey, key1, key3).block());
    }

    @Test
    void topK() {
        String key1 = "topK:1";
        String key2 = "topK:2";
        commands.unlink(key1, key2);
        RedisBloomCommands<String, String> topK = commands;
        assertEquals("OK", topK.topKReserve(key1, 3));
        List<Value<String>> result = topK.topKAdd(key1, "one", "two", "three");
        assertTrue(result.get(0).isEmpty());
        assertTrue(result.get(1).isEmpty());
        assertTrue(result.get(2).isEmpty());
        TopKInfo info = topK.topKInfo(key1);
        assertEquals(3, info.getK());
        assertEquals(7, info.getDepth());
        assertEquals(8, info.getWidth());
        assertEquals(.9, info.getDecay());
        result = topK.topKAdd(key1, "four", "four", "four");
        assertFalse(result.get(0).isEmpty());
        assertTrue(result.get(1).isEmpty());
        assertTrue(result.get(2).isEmpty());

        List<String> listResult = topK.topKList(key1);
        assertEquals("four", listResult.get(0));
        assertEquals("three", listResult.get(1));
        assertEquals("two", listResult.get(2));

        List<Boolean> queryResult = topK.topKQuery(key1, "four", "three", "two", "foo");
        assertEquals(true, queryResult.get(0));
        assertEquals(true, queryResult.get(1));
        assertEquals(true, queryResult.get(2));
        assertEquals(false, queryResult.get(3));

        List<KeyValue<String, Long>> listWithScores = topK.topKListWithScores(key1);
        assertEquals(3, listWithScores.get(0).getValue());
        assertEquals("four", listWithScores.get(0).getKey());
        assertEquals(1, listWithScores.get(1).getValue());
        assertEquals("three", listWithScores.get(1).getKey());
        assertEquals(1, listWithScores.get(2).getValue());
        assertEquals("two", listWithScores.get(2).getKey());
    }

    @Test
    void topKReactive() {
        String key1 = "topK:1";
        String key2 = "topK:2";
        commands.unlink(key1, key2);
        RedisBloomReactiveCommands<String, String> topK = connection.reactive();
        assertEquals("OK", topK.topKReserve(key1, 3).block());
        List<Value<String>> result = topK.topKAdd(key1, "one", "two", "three").collectList().block();
        assertNotNull(result);
        assertTrue(result.get(0).isEmpty());
        assertTrue(result.get(1).isEmpty());
        assertTrue(result.get(2).isEmpty());
        TopKInfo info = topK.topKInfo(key1).block();
        assertNotNull(info);
        assertEquals(3, info.getK());
        assertEquals(7, info.getDepth());
        assertEquals(8, info.getWidth());
        assertEquals(.9, info.getDecay());
        result = topK.topKAdd(key1, "four", "four", "four").collectList().block();
        assertNotNull(result);
        assertFalse(result.get(0).isEmpty());
        assertTrue(result.get(1).isEmpty());
        assertTrue(result.get(2).isEmpty());

        List<String> listResult = topK.topKList(key1).collectList().block();
        assertNotNull(listResult);
        assertEquals("four", listResult.get(0));
        assertEquals("three", listResult.get(1));
        assertEquals("two", listResult.get(2));

        List<Boolean> queryResult = topK.topKQuery(key1, "four", "three", "two", "foo").collectList().block();
        assertNotNull(queryResult);
        assertEquals(true, queryResult.get(0));
        assertEquals(true, queryResult.get(1));
        assertEquals(true, queryResult.get(2));
        assertEquals(false, queryResult.get(3));

        List<KeyValue<String, Long>> listWithScores = topK.topKListWithScores(key1).collectList().block();
        assertNotNull(listWithScores);
        assertEquals(KeyValue.just("four", 3L), listWithScores.get(0));
        assertEquals(KeyValue.just("three", 1L), listWithScores.get(1));
        assertEquals(KeyValue.just("two", 1L), listWithScores.get(2));
    }

    @Test
    void tDigestEmpty() {
        String key = "tdigest:empty";
        commands.unlink(key);
        assertEquals("OK", commands.tDigestCreate(key));
        double[] quantiles = { 0.1, 0.2, 0.3 };
        List<Double> res = commands.tDigestQuantile(key, quantiles);
        assertEquals(Double.NaN, res.get(0));
        assertEquals(Double.NaN, res.get(1));
        assertEquals(Double.NaN, res.get(2));

        res = commands.tDigestByRank(key, 4, 5, 6);
        assertEquals(Double.NaN, res.get(0));
        assertEquals(Double.NaN, res.get(1));
        assertEquals(Double.NaN, res.get(2));

        res = commands.tDigestByRevRank(key, 4, 5, 6);
        assertEquals(Double.NaN, res.get(0));
        assertEquals(Double.NaN, res.get(1));
        assertEquals(Double.NaN, res.get(2));

        res = commands.tDigestCdf(key, .4, .5);
        assertEquals(Double.NaN, res.get(0));
        assertEquals(Double.NaN, res.get(1));

        double singleResult = commands.tDigestMin(key);
        assertEquals(Double.NaN, singleResult);

        singleResult = commands.tDigestMax(key);
        assertEquals(Double.NaN, singleResult);

        singleResult = commands.tDigestTrimmedMean(key, 0, 1);
        assertEquals(Double.NaN, singleResult);
    }

    @Test
    void tDigestInf() {
        String key = "tdigest:inf";
        commands.unlink(key);
        RedisBloomCommands<String, String> tDigest = commands;
        assertEquals("OK", tDigest.tDigestCreate(key, 1000));
        assertEquals("OK", tDigest.tDigestAdd(key, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        assertEquals(Double.POSITIVE_INFINITY, tDigest.tDigestByRank(key, 25).get(0));
        assertEquals(Double.NEGATIVE_INFINITY, tDigest.tDigestByRevRank(key, 25).get(0));
    }

    @Test
    void tDigest() {
        String key = "tdigest:1";
        commands.unlink(key);
        RedisBloomCommands<String, String> tDigest = commands;
        assertEquals("OK", tDigest.tDigestCreate(key, 1000));
        assertEquals("OK", tDigest.tDigestAdd(key, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        List<Double> items = tDigest.tDigestByRank(key, 1, 5, 9);
        assertEquals(2, items.get(0));
        assertEquals(6, items.get(1));
        assertEquals(10, items.get(2));

        items = tDigest.tDigestByRevRank(key, 1, 5, 9);
        assertEquals(9, items.get(0));
        assertEquals(5, items.get(1));
        assertEquals(1, items.get(2));

        assertEquals(10, tDigest.tDigestMax(key));
        assertEquals(1, tDigest.tDigestMin(key));

        items = tDigest.tDigestCdf(key, 1, 5, 9);
        assertEquals(0.05, items.get(0), .01);
        assertEquals(.45, items.get(1), .01);
        assertEquals(.85, items.get(2), .01);

        TDigestInfo info = tDigest.tDigestInfo(key);
        assertEquals(1000, info.getCompression());
        assertEquals(10, info.getObservations());

        List<Double> quantiles = tDigest.tDigestQuantile(key, 0, 0.1);
        assertEquals(1, quantiles.get(0));
        assertEquals(2, quantiles.get(1));

        List<Long> ranks = tDigest.tDigestRank(key, -5, 100, 5.3);
        assertEquals(-1, ranks.get(0));
        assertEquals(10, ranks.get(1));
        assertEquals(5, ranks.get(2));

        ranks = tDigest.tDigestRevRank(key, -5, 100, 5.3);
        assertEquals(10, ranks.get(0));
        assertEquals(-1, ranks.get(1));
        assertEquals(5, ranks.get(2));

        Double trimmedMean = tDigest.tDigestTrimmedMean(key, .3, .7);
        assertEquals(5.5, trimmedMean);
    }

    @Test
    void tDigestReactive() {
        String key = "tdigest:1";
        commands.unlink(key);
        RedisBloomReactiveCommands<String, String> tDigest = connection.reactive();
        assertEquals("OK", tDigest.tDigestCreate(key, 1000).block());
        assertEquals("OK", tDigest.tDigestAdd(key, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).block());
        List<Double> items = tDigest.tDigestByRank(key, 1, 5, 9).collectList().block();
        assertNotNull(items);
        assertEquals(2, items.get(0));
        assertEquals(6, items.get(1));
        assertEquals(10, items.get(2));

        items = tDigest.tDigestByRevRank(key, 1, 5, 9).collectList().block();
        assertNotNull(items);
        assertEquals(9, items.get(0));
        assertEquals(5, items.get(1));
        assertEquals(1, items.get(2));

        assertEquals(10, tDigest.tDigestMax(key).block());
        assertEquals(1, tDigest.tDigestMin(key).block());

        items = tDigest.tDigestCdf(key, 1, 5, 9).collectList().block();
        assertNotNull(items);
        assertEquals(0.05, items.get(0), .01);
        assertEquals(.45, items.get(1), .01);
        assertEquals(.85, items.get(2), .01);

        TDigestInfo info = tDigest.tDigestInfo(key).block();
        assertNotNull(info);
        assertEquals(1000, info.getCompression());
        assertEquals(10, info.getObservations());

        List<Double> quantiles = tDigest.tDigestQuantile(key, 0, 0.1).collectList().block();
        assertNotNull(quantiles);
        assertEquals(1, quantiles.get(0));
        assertEquals(2, quantiles.get(1));

        List<Long> ranks = tDigest.tDigestRank(key, -5, 100, 5.3).collectList().block();
        assertNotNull(ranks);
        assertEquals(-1, ranks.get(0));
        assertEquals(10, ranks.get(1));
        assertEquals(5, ranks.get(2));

        ranks = tDigest.tDigestRevRank(key, -5, 100, 5.3).collectList().block();
        assertNotNull(ranks);
        assertEquals(10, ranks.get(0));
        assertEquals(-1, ranks.get(1));
        assertEquals(5, ranks.get(2));

        Double trimmedMean = tDigest.tDigestTrimmedMean(key, .3, .7).block();
        assertEquals(5.5, trimmedMean);
    }

    @Test
    void ftInfo() throws Exception {
        int count = populateIndex();
        IndexInfo info = indexInfo(INDEX);
        assertEquals(count, info.getNumDocs());
        List<FieldArgs<String>> fields = info.getFields();
        TextFieldArgs<String> descriptionField = (TextFieldArgs<String>) fields.get(5);
        assertEquals(DESCRIPTION, descriptionField.getName());
        Assertions.assertFalse(descriptionField.isNoIndex());
        Assertions.assertTrue(descriptionField.isNoStem());
        Assertions.assertFalse(descriptionField.isSortable());
        TagFieldArgs<String> styleField = (TagFieldArgs<String>) fields.get(2);
        assertEquals(STYLE, styleField.getName());
        Assertions.assertTrue(styleField.isSortable());
        assertTrue(styleField.getSeparator().isPresent());
        assertEquals(",", styleField.getSeparator().get());
    }

    private String jsonField(String name) {
        return "$." + name;
    }

    @Test
    void ftInfoFields() {
        String index = "indexFields";
        TagFieldArgs<String> idField = TagFieldArgs.<String> builder().name(jsonField(ID)).as(ID).separator("-").build();
        TextFieldArgs<String> nameField = TextFieldArgs.<String> builder().name(jsonField(NAME)).as(NAME).noIndex().noStem()
                .unNormalizedForm().weight(2).build();
        String styleFieldName = jsonField(STYLE);
        TextFieldArgs<String> styleField = TextFieldArgs.<String> builder().name(styleFieldName).as(styleFieldName).weight(1)
                .build();
        commands.ftCreate(index, CreateArgs.<String, String> builder().on(CreateArgs.TargetType.JSON).build(),
                Arrays.asList(idField, nameField, styleField));
        IndexInfo info = indexInfo(index);
        assertFieldEquals(idField, info.getFields().get(0));
        FieldArgs<String> actualNameField = info.getFields().get(1);
        // Workaround for older RediSearch versions (Redis Enterprise tests)
        assertFieldEquals(nameField, actualNameField);
        assertFieldEquals(styleField, info.getFields().get(2));
    }

    private void assertFieldEquals(FieldArgs<String> field1, FieldArgs<String> field2) {
        Assertions.assertEquals(field1.getFieldType(), field2.getFieldType());
        Assertions.assertEquals(field1.getAs(), field2.getAs());
        Assertions.assertEquals(field1.getName(), field2.getName());
        Assertions.assertEquals(field1.isIndexEmpty(), field2.isIndexEmpty());
        Assertions.assertEquals(field1.isNoIndex(), field2.isNoIndex());
        Assertions.assertEquals(field1.isSortable(), field2.isSortable());
        Assertions.assertEquals(field1.isUnNormalizedForm(), field2.isUnNormalizedForm());
        if (field1 instanceof TextFieldArgs<String>) {
            TextFieldArgs<String> textField1 = (TextFieldArgs<String>) field1;
            TextFieldArgs<String> textField2 = (TextFieldArgs<String>) field2;
            Assertions.assertEquals(textField1.getWeight(), textField2.getWeight());
            Assertions.assertEquals(textField1.isNoStem(), textField2.isNoStem());
            Assertions.assertEquals(textField1.getPhonetic(), textField2.getPhonetic());
        }
    }

    private IndexInfo indexInfo(String index) {
        return IndexInfo.parse(commands.ftInfo(index));
    }

    @Test
    void ftInfoOptions() {
        String index = "indexWithOptions";
        CreateArgs<String, String> createArgs = CreateArgs.<String, String> builder().on(CreateArgs.TargetType.JSON)
                .withPrefix("prefix1").withPrefix("prefix2").filter("@indexName==\"myindexname\"")
                .defaultLanguage(DocumentLanguage.CHINESE).languageField("languageField").defaultScore(.5)
                .scoreField("scoreField").payloadField("payloadField").maxTextFields().noOffsets().noHighlighting().noFields()
                .noFrequency().build();
        TagFieldArgs<String> idField = TagFieldArgs.<String> builder().name("id").build();
        NumericFieldArgs<String> scoreField = NumericFieldArgs.<String> builder().name("scoreField").build();
        commands.ftCreate(index, createArgs, Arrays.asList(idField, scoreField));
        IndexInfo info = IndexInfo.parse(commands.ftInfo(index));
        CreateArgs<String, String> actual = info.getIndexArgs();
        Assertions.assertEquals(createArgs.getOn(), actual.getOn());
        Assertions.assertEquals(createArgs.getPrefixes(), actual.getPrefixes());
        Assertions.assertEquals(createArgs.getFilter(), actual.getFilter());
        Assertions.assertEquals(createArgs.getDefaultLanguage(), actual.getDefaultLanguage());
        Assertions.assertEquals(createArgs.getLanguageField(), actual.getLanguageField());
        Assertions.assertEquals(createArgs.getDefaultScore(), actual.getDefaultScore());
        Assertions.assertEquals(createArgs.getScoreField(), actual.getScoreField());
        Assertions.assertEquals(createArgs.getPayloadField(), actual.getPayloadField());
    }

    @Test
    void utilsIndexInfo() {
        Assertions.assertThrows(RedisCommandExecutionException.class, () -> IndexInfo.parse(commands.ftInfo("wweriwjer")));
    }

}
