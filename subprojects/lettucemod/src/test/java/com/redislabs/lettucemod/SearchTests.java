package com.redislabs.lettucemod;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.redislabs.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redislabs.lettucemod.api.reactive.RedisModulesReactiveCommands;
import com.redislabs.lettucemod.api.sync.RedisModulesCommands;
import com.redislabs.lettucemod.search.*;
import com.redislabs.lettucemod.search.aggregate.GroupBy;
import com.redislabs.lettucemod.search.aggregate.Limit;
import com.redislabs.lettucemod.search.aggregate.SortBy;
import com.redislabs.lettucemod.search.aggregate.reducers.Avg;
import com.redislabs.lettucemod.search.aggregate.reducers.Count;
import com.redislabs.lettucemod.search.aggregate.reducers.ToList;
import com.redislabs.testcontainers.RedisServer;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SuppressWarnings("ConstantConditions")
public class SearchTests extends AbstractModuleTestBase {

    protected final static int BEER_COUNT = 2348;
    protected final static String SUGINDEX = "beersSug";

    public final static String BREWERY_ID = "brewery_id";
    public final static String ABV = "abv";
    public final static String ID = "id";
    public final static String NAME = "name";
    public final static String STYLE = "style";
    public final static Field[] SCHEMA = new Field[]{Field.text(NAME).matcher(Field.Text.PhoneticMatcher.English).build(), Field.tag(STYLE).sortable(true).build(), Field.numeric(ABV).sortable(true).build()};
    public final static String INDEX = "beers";
    private static final String KEYSPACE = "beer:";

    private static final List<Map<String, String>> beers = new ArrayList<>();

    protected static Map<String, String> mapOf(String... keyValues) {
        Map<String, String> map = new HashMap<>();
        for (int index = 0; index < keyValues.length / 2; index++) {
            map.put(keyValues[index], keyValues[index + 1]);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    @BeforeAll
    public static void loadBeers() throws IOException {
        CsvSchema schema = CsvSchema.builder().setUseHeader(true).setNullValue("").build();
        CsvMapper mapper = new CsvMapper();
        InputStream inputStream = SearchTests.class.getClassLoader().getResourceAsStream("beers.csv");
        mapper.readerFor(Map.class).with(schema).readValues(inputStream).forEachRemaining(e -> beers.add((Map<String, String>) e));
    }

    private void createBeerIndex(RedisServer redis) {
        RedisModulesCommands<String, String> sync = sync(redis);
        sync.flushall();
        sync.create(INDEX, CreateOptions.<String, String>builder().prefix(KEYSPACE).payloadField(BREWERY_ID).build(), SCHEMA);
        RedisModulesAsyncCommands<String, String> async = async(redis);
        async.setAutoFlushCommands(false);
        List<RedisFuture<?>> futures = new ArrayList<>();
        for (Map<String, String> beer : beers) {
            String key = KEYSPACE + beer.get(ID);
            futures.add(async.hset(key, beer));
        }
        async.flushCommands();
        async.setAutoFlushCommands(true);
        LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
    }

    private void createBeerSuggestions(RedisServer redis) {
        RedisModulesAsyncCommands<String, String> async = async(redis);
        async.setAutoFlushCommands(false);
        List<RedisFuture<?>> futures = new ArrayList<>();
        for (Map<String, String> beer : beers) {
            futures.add(async.sugadd(SUGINDEX, beer.get(NAME), 1));
        }
        async.flushCommands();
        async.setAutoFlushCommands(true);
        LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
    }

    @Test
    void testGeoLocation() {
        double longitude = -118.753604;
        double latitude = 34.027201;
        String locationString = "-118.753604,34.027201";
        RediSearchUtils.GeoLocation location = RediSearchUtils.GeoLocation.of(locationString);
        Assertions.assertEquals(longitude, location.getLongitude());
        Assertions.assertEquals(latitude, location.getLatitude());
        Assertions.assertEquals(locationString, RediSearchUtils.GeoLocation.toString(String.valueOf(longitude), String.valueOf(latitude)));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void testSugaddIncr(RedisServer redis) {
        RedisModulesCommands<String, String> sync = sync(redis);
        String key = "testSugadd";
        sync.sugadd(key, "value1", 1);
        sync.sugadd(key, "value1", 1, SugaddOptions.<String, String>builder().increment(true).build());
        List<Suggestion<String>> suggestions = sync.sugget(key, "value", SuggetOptions.builder().withScores(true).build());
        Assertions.assertEquals(1, suggestions.size());
        Assertions.assertEquals(1.4142135381698608, suggestions.get(0).getScore());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void testSugaddPayload(RedisServer redis) {
        RedisModulesCommands<String, String> sync = sync(redis);
        String key = "testSugadd";
        sync.sugadd(key, "value1", 1, SugaddOptions.<String, String>builder().payload("somepayload").build());
        List<Suggestion<String>> suggestions = sync.sugget(key, "value", SuggetOptions.builder().withPayloads(true).build());
        Assertions.assertEquals(1, suggestions.size());
        Assertions.assertEquals("somepayload", suggestions.get(0).getPayload());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void testSugaddScorePayload(RedisServer redis) {
        RedisModulesCommands<String, String> sync = sync(redis);
        String key = "testSugadd";
        sync.sugadd(key, "value1", 2, SugaddOptions.<String, String>builder().payload("somepayload").build());
        List<Suggestion<String>> suggestions = sync.sugget(key, "value", SuggetOptions.builder().withScores(true).withPayloads(true).build());
        Assertions.assertEquals(1, suggestions.size());
        Assertions.assertEquals(1.4142135381698608, suggestions.get(0).getScore());
        Assertions.assertEquals("somepayload", suggestions.get(0).getPayload());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void create(RedisServer redis) throws InterruptedException {
        createBeerIndex(redis);
        RedisModulesCommands<String, String> sync = sync(redis);
        String indexName = "hashIndex";
        sync.create(indexName, CreateOptions.<String, String>builder().prefix("beer:").on(CreateOptions.Structure.HASH).build(), SCHEMA);
        RedisModulesAsyncCommands<String, String> async = async(redis);
        async.setAutoFlushCommands(false);
        List<RedisFuture<?>> futures = new ArrayList<>();
        for (Map<String, String> beer : beers) {
            String id = beer.get(ID);
            futures.add(async.hmset("beer:" + id, beer));
        }
        async.flushCommands();
        LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
        Thread.sleep(1000);
        async.setAutoFlushCommands(true);
        IndexInfo info = RediSearchUtils.indexInfo(sync.indexInfo(indexName));
        Double numDocs = info.getNumDocs();
        assertEquals(2348, numDocs);

        CreateOptions<String, String> options = CreateOptions.<String, String>builder().prefix("release:").payloadField("xml").build();
        Field[] fields = new Field[]{Field.text("artist").sortable(true).build(), Field.tag("id").sortable(true).build(), Field.text("title").sortable(true).build()};
        sync.create("releases", options, fields);
        info = RediSearchUtils.indexInfo(sync.indexInfo("releases"));
        Assertions.assertEquals(fields.length, info.getFields().size());


        indexName = "temporaryIndex";
        sync.create(indexName, CreateOptions.<String, String>builder().temporary(1L).build(), Field.text("field1").build());

        assertEquals(indexName, sync.indexInfo(indexName).get(1));
        Thread.sleep(1501);
        try {
            sync.indexInfo(indexName);
        } catch (RedisCommandExecutionException e) {
            assertEquals("Unknown Index name", e.getMessage());
            return;
        }
        fail("Temporary index not deleted");


        sync.dropIndex(INDEX);
        // allow some time for the index to be deleted
        Thread.sleep(100);
        try {
            sync.search(INDEX, "*");
            fail("Index not dropped");
        } catch (RedisCommandExecutionException e) {
            // ignored, expected behavior
        }

        Map<String, Object> indexInfo = toMap(sync.indexInfo(INDEX));
        assertEquals(INDEX, indexInfo.get("index_name"));

        sync.alter(INDEX, Field.tag("newField").build());
        Map<String, String> doc = mapOf("newField", "value1");
        sync.hmset("beer:newDoc", doc);
        SearchResults<String, String> results = sync.search(INDEX, "@newField:{value1}");
        assertEquals(1, results.getCount());
        assertEquals(doc.get("newField"), results.get(0).get("newField"));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    public void testDropIndexDeleteDocs(RedisServer redis) throws InterruptedException {
        createBeerIndex(redis);
        RedisModulesCommands<String, String> sync = sync(redis);
        sync.dropIndexDeleteDocs(INDEX);
        Thread.sleep(1000);
        try {
            sync.indexInfo(INDEX);
            Assertions.fail("Expected unknown index exception");
        } catch (RedisCommandExecutionException e) {
            // ignore
        }
        List<String> keys = sync.keys(KEYSPACE + "*");
        Assertions.assertEquals(62, keys.size());
    }

    private Map<String, Object> toMap(List<Object> indexInfo) {
        Map<String, Object> map = new HashMap<>();
        Iterator<Object> iterator = indexInfo.iterator();
        while (iterator.hasNext()) {
            String key = (String) iterator.next();
            map.put(key, iterator.next());
        }
        return map;
    }


    @ParameterizedTest
    @MethodSource("redisServers")
    void list(RedisServer redis) throws ExecutionException, InterruptedException {
        RedisModulesCommands<String, String> sync = sync(redis);
        sync.flushall();
        Set<String> indexNames = new HashSet<>(Arrays.asList("index1", "index2", "index3"));
        for (String indexName : indexNames) {
            sync.create(indexName, Field.text("field1").sortable(true).build());
        }
        assertEquals(indexNames, new HashSet<>(sync.list()));
        assertEquals(indexNames, new HashSet<>(async(redis).list().get()));
        assertEquals(indexNames, new HashSet<>(reactive(redis).list().collectList().block()));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void search(RedisServer redis) throws ExecutionException, InterruptedException {
        createBeerIndex(redis);
        RedisModulesCommands<String, String> sync = sync(redis);
        SearchResults<String, String> results = sync.search(INDEX, "eldur");
        assertEquals(7, results.getCount());
        results = sync.search(INDEX, "Hefeweizen", SearchOptions.<String, String>builder().withScores(true).noContent(true).limit(new SearchOptions.Limit(0, 100)).build());
        assertEquals(22, results.getCount());
        assertEquals(22, results.size());
        assertTrue(results.get(0).getId().startsWith(KEYSPACE));
        assertTrue(results.get(0).getScore() > 0);

        results = sync.search(INDEX, "pale", SearchOptions.<String, String>builder().withPayloads(true).build());
        assertEquals(256, results.getCount());
        Document<String, String> result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.getPayload());
        assertEquals(sync.hget(result1.getId(), BREWERY_ID), result1.getPayload());

        results = sync.search(INDEX, "pale", SearchOptions.<String, String>builder().returnField(NAME).returnField(STYLE).build());
        assertEquals(256, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertNull(result1.get(ABV));
        SearchOptions<String, String> options = SearchOptions.<String, String>builder().withPayloads(true).noStopWords(true).limit(new SearchOptions.Limit(10, 100)).withScores(true).highlight(SearchOptions.Highlight.<String, String>builder().field(NAME).tags(SearchOptions.Tags.<String>builder().open("<TAG>").close("</TAG>").build()).build()).language(Language.English).noContent(false).sortBy(SearchOptions.SortBy.<String, String>field(NAME).order(Order.ASC)).verbatim(false).withSortKeys(true).returnField(NAME).returnField(STYLE).build();
        sync.search(INDEX, "pale", options);
        assertEquals(256, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertNull(result1.get(ABV));

        results = sync.search(INDEX, "pale", SearchOptions.<String, String>builder().returnField(NAME).returnField(STYLE).returnField("").build());
        assertEquals(256, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertNull(result1.get(ABV));

        results = sync.search(INDEX, "*", SearchOptions.<String, String>builder().inKeys(Collections.singletonList("beer:1018")).inKey("beer:2593").build());
        assertEquals(2, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertEquals("0.07", result1.get(ABV));

        results = sync.search(INDEX, "sculpin", SearchOptions.<String, String>builder().inField(NAME).build());
        assertEquals(2, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertEquals("0.07", result1.get(ABV));

        String term = "pale";
        String query = "@style:" + term;
        SearchOptions.Tags<String> tags = SearchOptions.Tags.<String>builder().open("<b>").close("</b>").build();
        results = sync.search(INDEX, query, SearchOptions.<String, String>builder().highlight(SearchOptions.Highlight.<String, String>builder().build()).build());
        for (Document<String, String> result : results) {
            assertTrue(highlighted(result, STYLE, tags, term));
        }
        results = sync.search(INDEX, query, SearchOptions.<String, String>builder().highlight(SearchOptions.Highlight.<String, String>builder().field(NAME).build()).build());
        for (Document<String, String> result : results) {
            assertFalse(highlighted(result, STYLE, tags, term));
        }
        tags = SearchOptions.Tags.<String>builder().open("[start]").close("[end]").build();
        results = sync.search(INDEX, query, SearchOptions.<String, String>builder().highlight(SearchOptions.Highlight.<String, String>builder().field(STYLE).tags(tags).build()).build());
        for (Document<String, String> result : results) {
            assertTrue(highlighted(result, STYLE, tags, term));
        }

        results = reactive(redis).search(INDEX, "pale", SearchOptions.<String, String>builder().limit(new SearchOptions.Limit(200, 100)).build()).block();
        assertEquals(256, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertNotNull(result1.get(ABV));

        results = sync.search(INDEX, "pail");
        assertEquals(256, results.getCount());

        results = sync.search(INDEX, "*", SearchOptions.<String, String>builder().limit(new SearchOptions.Limit(0, 0)).build());
        assertEquals(2348, results.getCount());

        String index = "escapeTagTestIdx";
        String idField = "id";
        RedisModulesAsyncCommands<String, String> async = async(redis);
        async.create(index, Field.tag(idField).build()).get();
        Map<String, String> doc1 = new HashMap<>();
        doc1.put(idField, "chris@blah.org,User1#test.org,usersdfl@example.com");
        async.hmset("doc1", doc1).get();
        results = async.search(index, "@id:{" + RediSearchUtils.escapeTag("User1#test.org") + "}").get();
        Assertions.assertEquals(1, results.size());

        SearchResults<String, String> filterResults = sync.search(INDEX, "*", SearchOptions.<String, String>builder().filter(SearchOptions.NumericFilter.<String, String>field(ABV).min(.08).max(.1)).build());
        Assertions.assertEquals(10, filterResults.size());
        for (Document<String, String> document : filterResults) {
            double abv = Double.parseDouble(document.get(ABV));
            Assertions.assertTrue(abv >= 0.08);
            Assertions.assertTrue(abv <= 0.1);
        }

    }

    @SuppressWarnings("SameParameterValue")
    private boolean highlighted(Document<String, String> result, String fieldName, SearchOptions.Tags<String> tags, String string) {
        String fieldValue = result.get(fieldName).toLowerCase();
        return fieldValue.contains(tags.getOpen() + string + tags.getClose());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void sugget(RedisServer redis) {
        createBeerSuggestions(redis);
        RedisModulesCommands<String, String> sync = sync(redis);
        RedisModulesReactiveCommands<String, String> reactive = reactive(redis);
        assertEquals(5, sync.sugget(SUGINDEX, "Ame").size());
        assertEquals(5, reactive.sugget(SUGINDEX, "Ame").collectList().block().size());
        SuggetOptions options = SuggetOptions.builder().max(1000L).build();
        assertEquals(8, sync.sugget(SUGINDEX, "Ame", options).size());
        assertEquals(8, reactive.sugget(SUGINDEX, "Ame", options).collectList().block().size());
        Consumer<List<Suggestion<String>>> withScores = results -> {
            assertEquals(8, results.size());
            assertEquals("American Hero", results.get(0).getString());
            assertEquals(.3, results.get(0).getScore(), .01);
        };
        SuggetOptions withScoresOptions = SuggetOptions.builder().max(1000L).withScores(true).build();
        withScores.accept(sync.sugget(SUGINDEX, "Ame", withScoresOptions));
        withScores.accept(reactive.sugget(SUGINDEX, "Ame", withScoresOptions).collectList().block());
        assertEquals(2305, sync.suglen(SUGINDEX));
        assertTrue(sync.sugdel(SUGINDEX, "American Hero"));
        assertTrue(reactive.sugdel(SUGINDEX, "American Lager").block());
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("redisServers")
    void aggregate(RedisServer redis) {
        createBeerIndex(redis);
        // Load tests
        Map<String, Map<String, String>> beerMap = beers.stream().collect(Collectors.toMap(b -> b.get(ID), b -> b));
        Consumer<AggregateResults<String>> loadAsserts = results -> {
            assertEquals(1, results.getCount());
            assertEquals(BEER_COUNT, results.size());
            for (Map<String, Object> result : results) {
                String id = (String) result.get(ID);
                Map<String, String> beer = beerMap.get(id);
                assertEquals(beer.get(NAME).toLowerCase(), ((String) result.get(NAME)).toLowerCase());
                String style = beer.get(STYLE);
                if (style != null) {
                    assertEquals(style.toLowerCase(), ((String) result.get(STYLE)).toLowerCase());
                }
            }

        };
        RedisModulesCommands<String, String> sync = sync(redis);
        RedisModulesReactiveCommands<String, String> reactive = reactive(redis);
        AggregateOptions<String, String> loadOptions = AggregateOptions.<String, String>builder().load(ID).load(NAME).load(STYLE).build();
        loadAsserts.accept(sync.aggregate(INDEX, "*", loadOptions));
        loadAsserts.accept(reactive.aggregate(INDEX, "*", loadOptions).block());

        // GroupBy tests
        Consumer<AggregateResults<String>> groupByAsserts = results -> {
            assertEquals(100, results.getCount());
            List<Double> abvs = results.stream().map(r -> Double.parseDouble((String) r.get(ABV))).collect(Collectors.toList());
            assertTrue(abvs.get(0) > abvs.get(abvs.size() - 1));
            assertEquals(20, results.size());
        };
        AggregateOptions<String, String> groupByOptions = AggregateOptions.operation(GroupBy.<String, String>property(STYLE).reducer(Avg.property(ABV).as(ABV).build()).build()).operation(SortBy.<String, String>property(SortBy.Property.name(ABV).order(Order.DESC)).build()).operation(Limit.<String, String>offset(0).num(20)).build();
        groupByAsserts.accept(sync.aggregate(INDEX, "*", groupByOptions));
        groupByAsserts.accept(reactive.aggregate(INDEX, "*", groupByOptions).block());

        Consumer<AggregateResults<String>> groupBy2Asserts = results -> {
            assertEquals(100, results.getCount());
            assertEquals("belgian ipa", ((String) results.get(0).get(STYLE)).toLowerCase());
            Object names = results.get(0).get("names");
            assertEquals(17, ((List<String>) names).size());
        };
        GroupBy<String, String> groupBy = GroupBy.<String, String>property(STYLE).reducer(ToList.property(NAME).as("names").build()).reducer(Count.as("count")).build();
        Limit<String, String> limit = Limit.<String, String>offset(0).num(1);
        AggregateOptions<String, String> groupBy2Options = AggregateOptions.operation(groupBy).operation(limit).build();
        groupBy2Asserts.accept(sync.aggregate(INDEX, "*", groupBy2Options));
        groupBy2Asserts.accept(reactive.aggregate(INDEX, "*", groupBy2Options).block());

        // Cursor tests
        Consumer<AggregateWithCursorResults<String>> cursorTests = cursorResults -> {
            assertEquals(1, cursorResults.getCount());
            assertEquals(1000, cursorResults.size());
//            assertEquals("harpoon ipa (2010)", ((String) cursorResults.get(999).get("name")).toLowerCase());
            assertTrue(Double.parseDouble((String) cursorResults.get(9).get("abv")) > 0);
        };
        AggregateOptions<String, String> cursorOptions = AggregateOptions.<String, String>builder().load(ID).load(NAME).load(ABV).build();
        AggregateWithCursorResults<String> cursorResults = sync.aggregate(INDEX, "*", Cursor.builder().build(), cursorOptions);
        cursorTests.accept(cursorResults);
        cursorTests.accept(reactive.aggregate(INDEX, "*", Cursor.builder().build(), cursorOptions).block());
        cursorResults = sync.cursorRead(INDEX, cursorResults.getCursor(), 500);
        assertEquals(500, cursorResults.size());
        cursorResults = reactive.cursorRead(INDEX, cursorResults.getCursor()).block();
        assertEquals(500, cursorResults.size());
        String deleteStatus = sync.cursorDelete(INDEX, cursorResults.getCursor());
        assertEquals("OK", deleteStatus);
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void alias(RedisServer redis) throws ExecutionException, InterruptedException {

        // SYNC
        createBeerIndex(redis);

        String alias = "alias123";

        RedisModulesCommands<String, String> sync = sync(redis);
        sync.aliasAdd(alias, INDEX);
        SearchResults<String, String> results = sync.search(alias, "*");
        assertTrue(results.size() > 0);

        String newAlias = "alias456";
        sync.aliasUpdate(newAlias, INDEX);
        assertTrue(sync.search(newAlias, "*").size() > 0);

        sync.aliasDel(newAlias);
        try {
            sync.search(newAlias, "*");
            fail("Alias was not removed");
        } catch (RedisCommandExecutionException e) {
            assertTrue(e.getMessage().contains("no such index") || e.getMessage().contains("Unknown Index name"));
        }

        sync.aliasDel(alias);
        RedisModulesAsyncCommands<String, String> async = async(redis);
        // ASYNC
        async.aliasAdd(alias, INDEX).get();
        results = async.search(alias, "*").get();
        assertTrue(results.size() > 0);

        async.aliasUpdate(newAlias, INDEX).get();
        assertTrue(async.search(newAlias, "*").get().size() > 0);

        async.aliasDel(newAlias).get();
        try {
            async.search(newAlias, "*").get();
            fail("Alias was not removed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause().getMessage().contains("no such index") || e.getCause().getMessage().contains("Unknown Index name"));
        }

        sync.aliasDel(alias);

        RedisModulesReactiveCommands<String, String> reactive = reactive(redis);
        // REACTIVE
        reactive.aliasAdd(alias, INDEX).block();
        results = reactive.search(alias, "*").block();
        assertTrue(results.size() > 0);

        reactive.aliasUpdate(newAlias, INDEX).block();
        results = reactive.search(newAlias, "*").block();
        Assertions.assertFalse(results.isEmpty());


        reactive.aliasDel(newAlias).block();
        try {
            reactive.search(newAlias, "*").block();
            fail("Alias was not removed");
        } catch (RedisCommandExecutionException e) {
            assertTrue(e.getMessage().contains("no such index") || e.getMessage().contains("Unknown Index name"));
        }

    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void info(RedisServer redis) throws ExecutionException, InterruptedException {
        createBeerIndex(redis);
        List<Object> infoList = async(redis).indexInfo(INDEX).get();
        IndexInfo info = RediSearchUtils.indexInfo(infoList);
        Assertions.assertEquals(2348, info.getNumDocs());
        List<Field> fields = info.getFields();
        Field.Text nameField = (Field.Text) fields.get(0);
        Assertions.assertEquals(NAME, nameField.getName());
        Assertions.assertFalse(nameField.isNoIndex());
        Assertions.assertFalse(nameField.isNoStem());
        Assertions.assertFalse(nameField.isSortable());
        Field.Tag styleField = (Field.Tag) fields.get(1);
        Assertions.assertEquals(STYLE, styleField.getName());
        Assertions.assertTrue(styleField.isSortable());
        Assertions.assertEquals(",", styleField.getSeparator());
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void tagVals(RedisServer redis) {
        createBeerIndex(redis);
        Set<String> TAG_VALS = new HashSet<>(Arrays.asList("american pale lager", "american pale ale (apa)", "american pale wheat ale", "american porter", "american pilsner", "american ipa", "american india pale lager", "american double / imperial ipa", "american double / imperial stout", "american double / imperial pilsner", "american dark wheat ale", "american barleywine", "american black ale", "american blonde ale", "american brown ale", "american stout", "american strong ale", "american amber / red ale", "american amber / red lager", "american adjunct lager", "american wild ale", "american white ipa", "american malt liquor", "altbier", "abbey single ale", "oatmeal stout", "other", "old ale", "saison / farmhouse ale", "schwarzbier", "scotch ale / wee heavy", "scottish ale", "smoked beer", "shandy", "belgian ipa", "belgian dark ale", "belgian strong dark ale", "belgian strong pale ale", "belgian pale ale", "berliner weissbier", "baltic porter", "bock", "bière de garde", "braggot", "cider", "california common / steam beer", "cream ale", "czech pilsener", "chile beer", "tripel", "winter warmer", "witbier", "wheat ale", "fruit / vegetable beer", "foreign / export stout", "flanders red ale", "flanders oud bruin", "english strong ale", "english stout", "english pale ale", "english pale mild ale", "english barleywine", "english brown ale", "english bitter", "english india pale ale (ipa)", "english dark mild ale", "extra special / strong bitter (esb)", "euro dark lager", "euro pale lager", "kölsch", "kristalweizen", "keller bier / zwickel bier", "milk / sweet stout", "munich helles lager", "munich dunkel lager", "märzen / oktoberfest", "mead", "maibock / helles bock", "german pilsener", "gose", "grisette", "pumpkin ale", "vienna lager", "rye beer", "radler", "rauchbier", "russian imperial stout", "roggenbier", "hefeweizen", "herbed / spiced beer", "dortmunder / export lager", "doppelbock", "dunkelweizen", "dubbel", "irish dry stout", "irish red ale", "quadrupel (quad)", "light lager", "low alcohol beer"));
        Assertions.assertEquals(TAG_VALS, new HashSet<>(sync(redis).tagVals(INDEX, STYLE)));
        Assertions.assertEquals(TAG_VALS, new HashSet<>(reactive(redis).tagVals(INDEX, STYLE).collectList().block()));
    }

    final static String[] DICT_TERMS = new String[]{"beer", "ale", "brew", "brewski"};

    @ParameterizedTest
    @MethodSource("redisServers")
    void dictadd(RedisServer redis) {
        RedisModulesCommands<String, String> sync = sync(redis);
        Assertions.assertEquals(DICT_TERMS.length, sync.dictadd("beers", DICT_TERMS));
        Assertions.assertEquals(new HashSet<>(Arrays.asList(DICT_TERMS)), new HashSet<>(sync.dictdump("beers")));
        Assertions.assertEquals(1, sync.dictdel("beers", "brew"));
        List<String> beerDict = new ArrayList<>();
        Collections.addAll(beerDict, DICT_TERMS);
        beerDict.remove("brew");
        Assertions.assertEquals(new HashSet<>(beerDict), new HashSet<>(sync.dictdump("beers")));
    }

    @ParameterizedTest
    @MethodSource("redisServers")
    void dictaddReactive(RedisServer redis) {
        RedisModulesReactiveCommands<String, String> reactive = reactive(redis);
        Assertions.assertEquals(DICT_TERMS.length, reactive.dictadd("beers", DICT_TERMS).block());
        Assertions.assertEquals(new HashSet<>(Arrays.asList(DICT_TERMS)), new HashSet<>(reactive.dictdump("beers").collectList().block()));
        Assertions.assertEquals(1, reactive.dictdel("beers", "brew").block());
        List<String> beerDict = new ArrayList<>();
        Collections.addAll(beerDict, DICT_TERMS);
        beerDict.remove("brew");
        Assertions.assertEquals(new HashSet<>(beerDict), new HashSet<>(reactive.dictdump("beers").collectList().block()));
    }

}
