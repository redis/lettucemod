package com.redislabs.mesclun;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.redislabs.mesclun.search.*;
import com.redislabs.mesclun.search.aggregate.GroupBy;
import com.redislabs.mesclun.search.aggregate.Limit;
import com.redislabs.mesclun.search.aggregate.SortBy;
import com.redislabs.mesclun.search.aggregate.reducers.Avg;
import com.redislabs.mesclun.search.aggregate.reducers.Count;
import com.redislabs.mesclun.search.aggregate.reducers.ToList;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;


@SuppressWarnings({"unchecked", "ConstantConditions"})
public class TestSearch extends BaseRedisModulesTest {

    protected final static int BEER_COUNT = 2348;
    protected final static String SUGINDEX = "beersSug";

    public final static String ABV = "abv";
    public final static String ID = "id";
    public final static String NAME = "name";
    public final static String STYLE = "style";
    public final static Field[] SCHEMA = new Field[]{Field.text(NAME).matcher(Field.Text.PhoneticMatcher.English).build(), Field.tag(STYLE).sortable(true).build(), Field.numeric(ABV).sortable(true).build()};
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
        InputStream inputStream = TestSearch.class.getClassLoader().getResourceAsStream("beers.csv");
        mapper.readerFor(Map.class).with(schema).readValues(inputStream).forEachRemaining(e -> beers.add((Map) e));
        return beers;
    }

    private List<Map<String, String>> createBeerIndex() throws IOException {
        sync.flushall();
        List<Map<String, String>> beers = beers();
        sync.create(INDEX, CreateOptions.<String, String>builder().payloadField(NAME).build(), SCHEMA);

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

    private void createBeerSuggestions() throws IOException {
        List<Map<String, String>> beers = beers();
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

    @Test
    void testSugaddIncr() {
        String key = "testSugadd";
        sync.sugadd(key, "value1", 1);
        sync.sugadd(key, "value1", 1, SugaddOptions.<String>builder().increment(true).build());
        List<Suggestion<String>> suggestions = sync.sugget(key, "value", SuggetOptions.builder().withScores(true).build());
        Assertions.assertEquals(1, suggestions.size());
        Assertions.assertEquals(1.4142135381698608, suggestions.get(0).getScore());
    }

    @Test
    void testSugaddPayload() {
        String key = "testSugadd";
        sync.sugadd(key, "value1", 1, SugaddOptions.<String>builder().payload("somepayload").build());
        List<Suggestion<String>> suggestions = sync.sugget(key, "value", SuggetOptions.builder().withPayloads(true).build());
        Assertions.assertEquals(1, suggestions.size());
        Assertions.assertEquals("somepayload", suggestions.get(0).getPayload());
    }

    @Test
    void testSugaddScorePayload() {
        String key = "testSugadd";
        sync.sugadd(key, "value1", 2, SugaddOptions.<String>builder().payload("somepayload").build());
        List<Suggestion<String>> suggestions = sync.sugget(key, "value", SuggetOptions.builder().withScores(true).withPayloads(true).build());
        Assertions.assertEquals(1, suggestions.size());
        Assertions.assertEquals(1.4142135381698608, suggestions.get(0).getScore());
        Assertions.assertEquals("somepayload", suggestions.get(0).getPayload());
    }

    @Test
    void create() throws IOException, InterruptedException {
        createBeerIndex();
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
        IndexInfo<String, String> info = RediSearchUtils.getInfo(sync.indexInfo(indexName));
        Double numDocs = info.getNumDocs();
        assertEquals(2348, numDocs);

        CreateOptions<String, String> options = CreateOptions.<String, String>builder().prefix("release:").payloadField("xml").build();
        Field[] fields = new Field[]{Field.text("artist").sortable(true).build(), Field.tag("id").sortable(true).build(), Field.text("title").sortable(true).build()};
        sync.create("releases", options, fields);
        info = RediSearchUtils.getInfo(sync.indexInfo("releases"));
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


        sync.dropIndex(INDEX, false);
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

    private Map<String, Object> toMap(List<Object> indexInfo) {
        Map<String, Object> map = new HashMap<>();
        Iterator<Object> iterator = indexInfo.iterator();
        while (iterator.hasNext()) {
            String key = (String) iterator.next();
            map.put(key, iterator.next());
        }
        return map;
    }


    @Test
    void list() throws ExecutionException, InterruptedException {
        sync.flushall();
        Set<String> indexNames = new HashSet<>(Arrays.asList("index1", "index2", "index3"));
        for (String indexName : indexNames) {
            sync.create(indexName, Field.text("field1").sortable(true).build());
        }
        assertEquals(indexNames, new HashSet<>(sync.list()));
        assertEquals(indexNames, new HashSet<>(async.list().get()));
        assertEquals(indexNames, new HashSet<>(reactive.list().collectList().block()));
    }


    @Test
    void search() throws IOException, ExecutionException, InterruptedException {
        createBeerIndex();

        SearchResults<String, String> results = sync.search(INDEX, "eldur");
        assertEquals(7, results.getCount());

        results = sync.search(INDEX, "Hefeweizen", SearchOptions.builder().withScores(true).noContent(true).limit(new SearchOptions.Limit(0, 100)).build());
        assertEquals(22, results.getCount());
        assertEquals(22, results.size());
        assertEquals("beer:1836", results.get(0).getId());
        assertEquals(12, results.get(0).getScore(), 0.000001);

        results = sync.search(INDEX, "pale", SearchOptions.builder().withPayloads(true).build());
        assertEquals(256, results.getCount());
        Document<String, String> result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.getPayload());
        assertEquals(result1.get(NAME), result1.getPayload());

        results = sync.search(INDEX, "pale", SearchOptions.builder().returnField(NAME).returnField(STYLE).build());
        assertEquals(256, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertNull(result1.get(ABV));
        SearchOptions options = SearchOptions.builder().withPayloads(true).noStopWords(true).limit(new SearchOptions.Limit(10, 100)).withScores(true).highlight(SearchOptions.Highlight.builder().field(NAME).tags(SearchOptions.Tags.builder().open("<TAG>").close("</TAG>").build()).build()).language(Language.English).noContent(false).sortBy(SearchOptions.SortBy.field(NAME).order(Order.ASC)).verbatim(false).withSortKeys(true).returnField(NAME).returnField(STYLE).build();
        sync.search(INDEX, "pale", options);
        assertEquals(256, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertNull(result1.get(ABV));

        results = sync.search(INDEX, "pale", SearchOptions.builder().returnField(NAME).returnField(STYLE).returnField("").build());
        assertEquals(256, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertNull(result1.get(ABV));

        results = sync.search(INDEX, "*", SearchOptions.builder().inKeys(Collections.singletonList("beer:1018")).inKey("beer:2593").build());
        assertEquals(2, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertEquals("0.07", result1.get(ABV));

        results = sync.search(INDEX, "sculpin", SearchOptions.builder().inField(NAME).build());
        assertEquals(2, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertEquals("0.07", result1.get(ABV));

        String term = "pale";
        String query = "@style:" + term;
        SearchOptions.Tags tags = SearchOptions.Tags.builder().open("<b>").close("</b>").build();
        results = sync.search(INDEX, query, SearchOptions.builder().highlight(SearchOptions.Highlight.builder().build()).build());
        for (Document<String, String> result : results) {
            assertTrue(highlighted(result, STYLE, tags, term));
        }
        results = sync.search(INDEX, query, SearchOptions.builder().highlight(SearchOptions.Highlight.builder().field(NAME).build()).build());
        for (Document<String, String> result : results) {
            assertFalse(highlighted(result, STYLE, tags, term));
        }
        tags = SearchOptions.Tags.builder().open("[start]").close("[end]").build();
        results = sync.search(INDEX, query, SearchOptions.builder().highlight(SearchOptions.Highlight.builder().field(STYLE).tags(tags).build()).build());
        for (Document<String, String> result : results) {
            assertTrue(highlighted(result, STYLE, tags, term));
        }

        results = reactive.search(INDEX, "pale", SearchOptions.builder().limit(new SearchOptions.Limit(200, 100)).build()).block();
        assertEquals(256, results.getCount());
        result1 = results.get(0);
        assertNotNull(result1.get(NAME));
        assertNotNull(result1.get(STYLE));
        assertNotNull(result1.get(ABV));

        results = sync.search(INDEX, "pail");
        assertEquals(256, results.getCount());

        results = sync.search(INDEX, "*", SearchOptions.builder().limit(new SearchOptions.Limit(0, 0)).build());
        assertEquals(2348, results.getCount());

        String index = "escapeTagTestIdx";
        String idField = "id";
        async.create(index, Field.tag(idField).build()).get();
        Map<String, String> doc1 = new HashMap<>();
        doc1.put(idField, "chris@blah.org,User1#test.org,usersdfl@example.com");
        async.hmset("doc1", doc1).get();
        results = async.search(index, "@id:{" + RediSearchUtils.escapeTag("User1#test.org") + "}").get();
        Assertions.assertEquals(1, results.size());

        SearchResults<String, String> filterResults = sync.search(INDEX, "*", SearchOptions.builder().filter(SearchOptions.NumericFilter.field(ABV).min(.08).max(.1)).build());
        Assertions.assertEquals(10, filterResults.size());
        for (Document<String, String> document : filterResults) {
            double abv = Double.parseDouble(document.get(ABV));
            Assertions.assertTrue(abv >= 0.08);
            Assertions.assertTrue(abv <= 0.1);
        }

    }

    @SuppressWarnings("SameParameterValue")
    private boolean highlighted(Document<String, String> result, String fieldName, SearchOptions.Tags tags, String string) {
        String fieldValue = result.get(fieldName).toLowerCase();
        return fieldValue.contains(tags.getOpen() + string + tags.getClose());
    }

    @Test
    void sugget() throws IOException {
        createBeerSuggestions();
        List<Suggestion<String>> results = sync.sugget(SUGINDEX, "Ame");
        assertEquals(5, results.size());
        results = sync.sugget(SUGINDEX, "Ame", SuggetOptions.builder().max(1000L).build());
        assertEquals(8, results.size());
        results = sync.sugget(SUGINDEX, "Ame", SuggetOptions.builder().max(1000L).withScores(true).build());
        assertEquals(8, results.size());
        assertEquals("American Hero", results.get(0).getString());
        assertEquals(2305, sync.suglen(SUGINDEX));
        assertTrue(sync.sugdel(SUGINDEX, "American Hero"));
    }

    @Test
    void aggregate() throws IOException {
        List<Map<String, String>> beers = createBeerIndex();
        // Load tests
        AggregateResults<String> results = sync.aggregate(INDEX, "*", AggregateOptions.builder().load(ID).load(NAME).load(STYLE).build());
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
        results = sync.aggregate(INDEX, "*", AggregateOptions.builder().operation(GroupBy.property(STYLE).reducer(Avg.property(ABV).as(ABV).build()).build()).operation(SortBy.property(SortBy.Property.name(ABV).order(Order.DESC)).build()).operation(Limit.offset(0).num(20)).build());
        assertEquals(100, results.getCount());
        List<Double> abvs = results.stream().map(r -> Double.parseDouble((String) r.get(ABV))).collect(Collectors.toList());
        assertTrue(abvs.get(0) > abvs.get(abvs.size() - 1));
        assertEquals(20, results.size());
        results = sync.aggregate(INDEX, "*", AggregateOptions.builder().operation(GroupBy.property(STYLE).reducer(ToList.property(NAME).as("names").build()).reducer(Count.as("count")).build()).operation(Limit.offset(0).num(1)).build());
        assertEquals(100, results.getCount());
        assertEquals("belgian ipa", ((String) results.get(0).get(STYLE)).toLowerCase());
        Object names = results.get(0).get("names");
        assertEquals(17, ((List<String>) names).size());

        // Cursor tests
        AggregateWithCursorResults<String> cursorResults = sync.aggregate(INDEX, "*", Cursor.builder().build(), AggregateOptions.builder().load(ID).load(NAME).load(ABV).build());
        assertEquals(1, cursorResults.getCount());
        assertEquals(1000, cursorResults.size());
        assertEquals("harpoon ipa (2010)", ((String) cursorResults.get(999).get("name")).toLowerCase());
        assertEquals("0.086", cursorResults.get(9).get("abv"));
        cursorResults = sync.cursorRead(INDEX, cursorResults.getCursor());
        assertEquals(1000, cursorResults.size());
        String deleteStatus = sync.cursorDelete(INDEX, cursorResults.getCursor());
        assertEquals("OK", deleteStatus);
    }

    @Test
    void alias() throws IOException, ExecutionException, InterruptedException {

        // SYNC
        createBeerIndex();

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

    @Test
    void info() throws IOException, ExecutionException, InterruptedException {
        createBeerIndex();
        List<Object> infoList = async.indexInfo(INDEX).get();
        IndexInfo<String, String> info = RediSearchUtils.getInfo(infoList);
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


}
