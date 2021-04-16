package com.redislabs.mesclun;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.redislabs.mesclun.search.*;
import com.redislabs.testcontainers.BaseRedisModulesTest;
import com.redislabs.testcontainers.RedisModulesContainer;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("unchecked")
@Testcontainers
public class TestRediSearch extends BaseRedisModulesTest {

    protected final static int BEER_COUNT = 2348;
    protected final static String SUGINDEX = "beersSug";

    public final static String ABV = "abv";
    public final static String ID = "id";
    public final static String NAME = "name";
    public final static String STYLE = "style";
    public final static Field<String, String>[] SCHEMA = new Field[]{Field.text(NAME).matcher(Field.Text.PhoneticMatcher.English).build(), Field.tag(STYLE).sortable(true).build(), Field.numeric(ABV).sortable(true).build()};
    public final static String INDEX = "beers";

    protected static Map<String, String> mapOf(String... keyValues) {
        Map<String, String> map = new HashMap<>();
        for (int index = 0; index < keyValues.length / 2; index++) {
            map.put(keyValues[index], keyValues[index + 1]);
        }
        return map;
    }

    @SuppressWarnings("rawtypes")
    private List<Map<String, String>> beers() throws IOException {
        List<Map<String, String>> beers = new ArrayList<>();
        CsvSchema schema = CsvSchema.builder().setUseHeader(true).setNullValue("").build();
        CsvMapper mapper = new CsvMapper();
        InputStream inputStream = TestRediSearch.class.getClassLoader().getResourceAsStream("beers.csv");
        mapper.readerFor(Map.class).with(schema).readValues(inputStream).forEachRemaining(e -> beers.add((Map) e));
        return beers;
    }

    private List<Map<String, String>> createBeerIndex(RedisModulesContainer container) throws IOException, ExecutionException, InterruptedException {
        RedisModulesCommands<String, String> sync = container.sync();
        sync.flushall();
        List<Map<String, String>> beers = beers();
        sync.create(INDEX, CreateOptions.<String, String>builder().payloadField(NAME).build(), SCHEMA);

        RedisModulesAsyncCommands<String, String> async = container.async();
        async.setAutoFlushCommands(false);
        List<RedisFuture<?>> futures = new ArrayList<>();
        for (Map<String, String> beer : beers) {
            futures.add(async.hmset("beer:" + beer.get(ID), beer));
        }
        async.flushCommands();
        async.setAutoFlushCommands(true);
        LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
        return beers;
    }

    private void createBeerSuggestions(RedisModulesContainer container) throws IOException {
        List<Map<String, String>> beers = beers();
        RedisModulesAsyncCommands<String, String> async = container.async();
        async.setAutoFlushCommands(false);
        List<RedisFuture<?>> futures = new ArrayList<>();
        for (Map<String, String> beer : beers) {
            futures.add(async.sugadd(SUGINDEX, beer.get(NAME), 1));
        }
        async.flushCommands();
        async.setAutoFlushCommands(true);
        LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
    }

    @ParameterizedTest
    @MethodSource("containers")
    void create(RedisModulesContainer redisContainer) throws IOException, ExecutionException, InterruptedException {
        RedisModulesAsyncCommands<String, String> async = redisContainer.async();
        RedisModulesCommands<String, String> sync = redisContainer.sync();
        RedisModulesReactiveCommands<String, String> reactive = redisContainer.reactive();

        createBeerIndex(redisContainer);
        String indexName = "hashIndex";
        sync.create(indexName, CreateOptions.<String, String>builder().prefix("beer:").on(CreateOptions.Structure.HASH).build(), SCHEMA);
        List<Map<String, String>> beers = beers();
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
        IndexInfo<String, String> info = RediSearchUtils.getInfo(sync.ftInfo(indexName));
        Double numDocs = info.getNumDocs();
        assertEquals(2348, numDocs);

        CreateOptions<String, String> options = CreateOptions.<String, String>builder().prefix("release:").payloadField("xml").build();
        Field<String, String>[] fields = new Field[]{Field.text("artist").sortable(true).build(), Field.tag("id").sortable(true).build(), Field.text("title").sortable(true).build()};
        sync.create("releases", options, fields);
        info = RediSearchUtils.getInfo(sync.ftInfo("releases"));
        Assertions.assertEquals(fields.length, info.getFields().size());


        indexName = "temporaryIndex";
        sync.create(indexName, CreateOptions.<String, String>builder().temporary(1L).build(), Field.text("field1").build());

        assertEquals(indexName, sync.ftInfo(indexName).get(1));
        Thread.sleep(1501);
        try {
            sync.ftInfo(indexName);
        } catch (RedisCommandExecutionException e) {
            assertEquals("Unknown Index name", e.getMessage());
            return;
        }
        fail("Temporary index not deleted");


        sync.dropIndex(INDEX, false);
        // allow some time for the index to be deleted
        Thread.sleep(100);
        try {
            sync.search(INDEX, "*");
            fail("Index not dropped");
        } catch (RedisCommandExecutionException e) {
            // ignored, expected behavior
        }

        Map<String, Object> indexInfo = toMap(sync.ftInfo(INDEX));
        assertEquals(INDEX, indexInfo.get("index_name"));

        sync.alter(INDEX, Field.tag("newField").build());
        Map<String, String> doc = mapOf("newField", "value1");
        sync.hmset("beer:newDoc", doc);
        SearchResults<String, String> results = sync.search(INDEX, "@newField:{value1}");
        assertEquals(1, results.getCount());
        assertEquals(doc.get("newField"), results.get(0).get("newField"));
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
    @MethodSource("containers")
    void list(RedisModulesContainer redisContainer) throws ExecutionException, InterruptedException {
        RedisModulesAsyncCommands<String, String> async = redisContainer.async();
        RedisModulesCommands<String, String> sync = redisContainer.sync();
        RedisModulesReactiveCommands<String, String> reactive = redisContainer.reactive();
        sync.flushall();
        Set<String> indexNames = new HashSet<>(Arrays.asList("index1", "index2", "index3"));
        for (String indexName : indexNames) {
            sync.create(indexName, Field.text("field1").sortable(true).build());
        }
        assertEquals(indexNames, new HashSet<>(sync.list()));
        assertEquals(indexNames, new HashSet<>(async.list().get()));
        assertEquals(indexNames, new HashSet<>(reactive.list().collectList().block()));
    }

    @ParameterizedTest
    @MethodSource("containers")
    void search(RedisModulesContainer redisContainer) throws IOException, ExecutionException, InterruptedException {
        createBeerIndex(redisContainer);

        RedisModulesAsyncCommands<String, String> async = redisContainer.async();
        RedisModulesCommands<String, String> sync = redisContainer.sync();
        RedisModulesReactiveCommands<String, String> reactive = redisContainer.reactive();

        SearchResults<String, String> results = sync.search(INDEX, "eldur");
        assertEquals(7, results.getCount());

        results = sync.search(INDEX, "Hefeweizen", SearchOptions.<String, String>builder().withScores(true).noContent(true).limit(new SearchOptions.Limit(0, 100)).build());
        assertEquals(22, results.getCount());
        assertEquals(22, results.size());
        assertEquals("beer:1836", results.get(0).getId());
        assertEquals(12, results.get(0).getScore(), 0.000001);

        results = sync.search(INDEX, "pale", SearchOptions.<String, String>builder().withPayloads(true).build());
        assertEquals(256, results.getCount());
        Document<String, String> result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.getPayload());
        assertEquals(result1.get(NAME), result1.getPayload());

        results = sync.search(INDEX, "pale", SearchOptions.<String, String>builder().returnField(NAME).returnField(STYLE).build());
        assertEquals(256, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertNull(result1.get(ABV));

        SearchOptions<String, String> options = SearchOptions.<String, String>builder().withPayloads(true).noStopWords(true).limit(new SearchOptions.Limit(10, 100)).withScores(true).highlight(SearchOptions.Highlight.<String, String>builder().field(NAME).tag(SearchOptions.Highlight.Tag.<String>builder().open("<TAG>").close("</TAG>").build()).build()).language(SearchOptions.Language.English).noContent(false).sortBy(SearchOptions.SortBy.<String, String>builder().direction(SearchOptions.SortBy.Direction.Ascending).field(NAME).build()).verbatim(false).withSortKeys(true).returnField(NAME).returnField(STYLE).build();
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
        SearchOptions.Highlight.Tag<String> tag = SearchOptions.Highlight.Tag.<String>builder().open("<b>").close("</b>").build();
        results = sync.search(INDEX, query, SearchOptions.<String, String>builder().highlight(SearchOptions.Highlight.<String, String>builder().build()).build());
        for (Document<String, String> result : results) {
            assertTrue(highlighted(result, STYLE, tag, term));
        }
        results = sync.search(INDEX, query, SearchOptions.<String, String>builder().highlight(SearchOptions.Highlight.<String, String>builder().field(NAME).build()).build());
        for (Document<String, String> result : results) {
            assertFalse(highlighted(result, STYLE, tag, term));
        }
        tag = SearchOptions.Highlight.Tag.<String>builder().open("[start]").close("[end]").build();
        results = sync.search(INDEX, query, SearchOptions.<String, String>builder().highlight(SearchOptions.Highlight.<String, String>builder().field(STYLE).tag(tag).build()).build());
        for (Document<String, String> result : results) {
            assertTrue(highlighted(result, STYLE, tag, term));
        }

        results = reactive.search(INDEX, "pale", SearchOptions.<String, String>builder().limit(new SearchOptions.Limit(200, 100)).build()).block();
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
        async.create(index, Field.tag(idField).build()).get();
        Map<String, String> doc1 = new HashMap<>();
        doc1.put(idField, "chris@blah.org,User1#test.org,usersdfl@example.com");
        async.hmset("doc1", doc1).get();
        results = async.search(index, "@id:{" + RediSearchUtils.escapeTag("User1#test.org") + "}").get();
        Assertions.assertEquals(1, results.size());

    }

    private boolean highlighted(Document<String, String> result, String fieldName, SearchOptions.Highlight.Tag<String> tag, String string) {
        String fieldValue = result.get(fieldName).toLowerCase();
        return fieldValue.contains(tag.getOpen() + string + tag.getClose());
    }

    @ParameterizedTest
    @MethodSource("containers")
    void sugget(RedisModulesContainer redisContainer) throws IOException {
        createBeerSuggestions(redisContainer);
        RediSearchCommands<String, String> search = redisContainer.sync();
        List<Suggestion<String>> results = search.sugget(SUGINDEX, "Ame");
        assertEquals(5, results.size());
        results = search.sugget(SUGINDEX, "Ame", SuggetOptions.builder().max(1000L).build());
        assertEquals(8, results.size());
        results = search.sugget(SUGINDEX, "Ame", SuggetOptions.builder().max(1000L).withScores(true).build());
        assertEquals(8, results.size());
        assertEquals("American Hero", results.get(0).getString());
        assertEquals(2305, search.suglen(SUGINDEX));
        assertTrue(search.sugdel(SUGINDEX, "American Hero"));
    }

    @ParameterizedTest
    @MethodSource("containers")
    void aggregate(RedisModulesContainer redisContainer) throws IOException, ExecutionException, InterruptedException {
        List<Map<String, String>> beers = createBeerIndex(redisContainer);
        RediSearchCommands<String, String> search = redisContainer.sync();

        // Load tests
        AggregateResults<String> results = search.aggregate(INDEX, "*", AggregateOptions.<String, String>builder().load(ID).load(NAME).load(STYLE).build());
        Assertions.assertEquals(1, results.getCount());
        assertEquals(BEER_COUNT, results.size());
        Map<String, Map<String, String>> beerMap = beers.stream().collect(Collectors.toMap(b -> b.get(ID), b -> b));
        for (Map<String, Object> result : results) {
            String id = (String) result.get(ID);
            Map<String, String> beer = beerMap.get(id);
            assertEquals(beer.get(NAME).toLowerCase(), ((String) result.get(NAME)).toLowerCase());
            String style = beer.get(STYLE);
            if (style != null) {
                assertEquals(style.toLowerCase(), ((String) result.get(STYLE)).toLowerCase());
            }
        }

        // GroupBy tests
        results = search.aggregate(INDEX, "*", AggregateOptions.<String, String>builder().groupBy(Collections.singletonList(STYLE), AggregateOptions.Operation.GroupBy.Reducer.Avg.<String, String>builder().property(ABV).as(ABV).build()).sortBy(AggregateOptions.Operation.SortBy.Property.<String, String>builder().property(ABV).order(AggregateOptions.Operation.Order.Desc).build()).limit(0, 20).build());
        assertEquals(100, results.getCount());
        List<Double> abvs = results.stream().map(r -> Double.parseDouble((String) r.get(ABV))).collect(Collectors.toList());
        assertTrue(abvs.get(0) > abvs.get(abvs.size() - 1));
        assertEquals(20, results.size());
        results = search.aggregate(INDEX, "*", AggregateOptions.<String, String>builder().groupBy(Collections.singletonList(STYLE), AggregateOptions.Operation.GroupBy.Reducer.ToList.<String, String>builder().property(NAME).as("names").build(), AggregateOptions.Operation.GroupBy.Reducer.Count.of("count")).limit(0, 1).build());
        assertEquals(100, results.getCount());
        assertEquals("belgian ipa", ((String) results.get(0).get(STYLE)).toLowerCase());
        Object names = results.get(0).get("names");
        assertEquals(17, ((List<String>) names).size());

        // Cursor tests
        AggregateWithCursorResults<String> cursorResults = search.aggregate(INDEX, "*", Cursor.builder().build(), AggregateOptions.<String, String>builder().load(ID).load(NAME).load(ABV).build());
        assertEquals(1, cursorResults.getCount());
        assertEquals(1000, cursorResults.size());
        assertEquals("harpoon ipa (2010)", ((String) cursorResults.get(999).get("name")).toLowerCase());
        assertEquals("0.086", cursorResults.get(9).get("abv"));
        cursorResults = search.cursorRead(INDEX, cursorResults.getCursor());
        assertEquals(1000, cursorResults.size());
        String deleteStatus = search.cursorDelete(INDEX, cursorResults.getCursor());
        assertEquals("OK", deleteStatus);
    }

    @ParameterizedTest
    @MethodSource("containers")
    void alias(RedisModulesContainer redisContainer) throws IOException, ExecutionException, InterruptedException {

        // SYNC
        RedisModulesCommands<String, String> sync = redisContainer.sync();
        createBeerIndex(redisContainer);

        String alias = "alias123";

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

        // ASYNC
        RedisModulesAsyncCommands<String, String> async = redisContainer.async();
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

        // REACTIVE
        RedisModulesReactiveCommands<String, String> reactive = redisContainer.reactive();
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
    @MethodSource("containers")
    void info(RedisModulesContainer redisContainer) throws IOException, ExecutionException, InterruptedException {
        createBeerIndex(redisContainer);
        RedisModulesAsyncCommands<String, String> async = redisContainer.async();
        List<Object> infoList = async.ftInfo(INDEX).get();
        IndexInfo<String, String> info = RediSearchUtils.getInfo(infoList);
        Assertions.assertEquals(2348, info.getNumDocs());
        List<Field<String, String>> fields = info.getFields();
        Field.Text<String, String> nameField = (Field.Text<String, String>) fields.get(0);
        Assertions.assertEquals(NAME, nameField.getName());
        Assertions.assertFalse(nameField.isNoIndex());
        Assertions.assertFalse(nameField.isNoStem());
        Assertions.assertFalse(nameField.isSortable());
        Field.Tag<String, String> styleField = (Field.Tag<String, String>) fields.get(1);
        Assertions.assertEquals(STYLE, styleField.getName());
        Assertions.assertTrue(styleField.isSortable());
        Assertions.assertEquals(",", styleField.getSeparator());
    }


}
