package com.redis.lettucemod;

import static com.redis.lettucemod.Beers.ABV;
import static com.redis.lettucemod.Beers.DESCRIPTION;
import static com.redis.lettucemod.Beers.IBU;
import static com.redis.lettucemod.Beers.ID;
import static com.redis.lettucemod.Beers.INDEX;
import static com.redis.lettucemod.Beers.NAME;
import static com.redis.lettucemod.Beers.PREFIX;
import static com.redis.lettucemod.Beers.STYLE;
import static com.redis.lettucemod.Beers.jsonNodeIterator;
import static com.redis.lettucemod.Beers.mapIterator;
import static com.redis.lettucemod.Beers.populateIndex;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.reactive.RedisBloomReactiveCommands;
import com.redis.lettucemod.api.reactive.RedisModulesReactiveCommands;
import com.redis.lettucemod.api.sync.RedisBloomCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.api.sync.RedisTimeSeriesCommands;
import com.redis.lettucemod.bloom.BloomFilterInfo;
import com.redis.lettucemod.bloom.BloomFilterInfoType;
import com.redis.lettucemod.bloom.BloomFilterInsertOptions;
import com.redis.lettucemod.bloom.CmsInfo;
import com.redis.lettucemod.bloom.CuckooFilter;
import com.redis.lettucemod.bloom.CuckooFilterInsertOptions;
import com.redis.lettucemod.bloom.LongScoredValue;
import com.redis.lettucemod.bloom.TDigestInfo;
import com.redis.lettucemod.bloom.TopKInfo;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateOptions.Load;
import com.redis.lettucemod.search.AggregateResults;
import com.redis.lettucemod.search.AggregateWithCursorResults;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.CreateOptions.DataType;
import com.redis.lettucemod.search.CursorOptions;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.GeoLocation;
import com.redis.lettucemod.search.Group;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.Language;
import com.redis.lettucemod.search.Limit;
import com.redis.lettucemod.search.Reducers.Avg;
import com.redis.lettucemod.search.Reducers.Count;
import com.redis.lettucemod.search.Reducers.Max;
import com.redis.lettucemod.search.Reducers.ToList;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchOptions.Highlight;
import com.redis.lettucemod.search.SearchOptions.Highlight.Tags;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.Sort;
import com.redis.lettucemod.search.Sort.Property;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.lettucemod.search.TagField;
import com.redis.lettucemod.search.TextField;
import com.redis.lettucemod.search.VectorField;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.Aggregator;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.KeyValue;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.Value;
import io.lettuce.core.json.JsonPath;
import reactor.core.publisher.Mono;

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

	private static final String SUGINDEX = "beersSug";

	private static final String PONG = "PONG";

	protected static Map<String, String> mapOf(String... keyValues) {
		Map<String, String> map = new HashMap<>();
		for (int index = 0; index < keyValues.length / 2; index++) {
			map.put(keyValues[index * 2], keyValues[index * 2 + 1]);
		}
		return map;
	}

	protected StatefulRedisModulesConnection<String, String> connection;

	private AbstractRedisClient client;

	@BeforeAll
	void setup() {
		RedisServer server = getRedisServer();
		if (server instanceof Startable) {
			((Startable) server).start();
		}
		client = redisClient(getRedisServer());
		connection = RedisModulesUtils.connection(client);
	}

	private AbstractRedisClient redisClient(RedisServer server) {
		if (server.isRedisCluster()) {
			return RedisModulesClusterClient.create(server.getRedisURI());
		}
		return RedisModulesClient.create(server.getRedisURI());
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
	void setupRedis() {
		connection.sync().flushall();
	}

	protected abstract RedisServer getRedisServer();

	protected void assertPing(StatefulRedisModulesConnection<String, String> connection) {
		assertEquals(PONG, ping(connection));
	}

	private void createBeerSuggestions() throws IOException {
		MappingIterator<Map<String, Object>> beers = mapIterator();
		try {
			connection.setAutoFlushCommands(false);
			List<RedisFuture<?>> futures = new ArrayList<>();
			RedisModulesAsyncCommands<String, String> async = connection.async();
			while (beers.hasNext()) {
				Map<String, Object> beer = beers.next();
				futures.add(async.ftSugadd(SUGINDEX, Suggestion.string((String) beer.get(NAME)).score(1).build()));
			}
			connection.flushCommands();
			LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
		} finally {
			connection.setAutoFlushCommands(true);
		}
	}

	protected String ping(StatefulRedisModulesConnection<String, String> connection) {
		return connection.sync().ping();
	}

	@Test
	void sugaddIncr() {
		RedisModulesCommands<String, String> sync = connection.sync();
		String key = "testSugadd";
		sync.ftSugadd(key, Suggestion.of("value1", 1));
		sync.ftSugaddIncr(key, Suggestion.of("value1", 1));
		List<Suggestion<String>> suggestions = sync.ftSugget(key, "value",
				SuggetOptions.builder().withScores(true).build());
		assertEquals(1, suggestions.size());
		assertEquals(1.4142135381698608, suggestions.get(0).getScore());
	}

	@Test
	void sugaddPayload() {
		RedisModulesCommands<String, String> sync = connection.sync();
		String key = "testSugadd";
		sync.ftSugadd(key, Suggestion.string("value1").score(1).payload("somepayload").build());
		List<Suggestion<String>> suggestions = sync.ftSugget(key, "value",
				SuggetOptions.builder().withPayloads(true).build());
		assertEquals(1, suggestions.size());
		assertEquals("somepayload", suggestions.get(0).getPayload());
	}

	@Test
	void sugaddScorePayload() {
		RedisModulesCommands<String, String> sync = connection.sync();
		String key = "testSugadd";
		sync.ftSugadd(key, Suggestion.string("value1").score(2).payload("somepayload").build());
		List<Suggestion<String>> suggestions = sync.ftSugget(key, "value",
				SuggetOptions.builder().withScores(true).withPayloads(true).build());
		assertEquals(1, suggestions.size());
		assertEquals(1.4142135381698608, suggestions.get(0).getScore());
		assertEquals("somepayload", suggestions.get(0).getPayload());
	}

	@Test
	void ftInfoInexistentIndex() {
		Assertions.assertThrows(RedisCommandExecutionException.class, () -> connection.sync().ftInfo("sdfsdfs"),
				RedisModulesUtils.ERROR_UNKNOWN_INDEX_NAME);
	}

	@Test
	@SuppressWarnings("unchecked")
	void ftCreate() throws Exception {
		int count = populateIndex(connection);
		RedisModulesCommands<String, String> sync = connection.sync();
		assertEquals(count, RedisModulesUtils.indexInfo(sync.ftInfo(INDEX)).getNumDocs());
		CreateOptions<String, String> options = CreateOptions.<String, String>builder().prefix("release:")
				.payloadField("xml").build();
		List<Field<String>> fields = Arrays.asList(Field.text("artist").sortable().build(),
				Field.tag("id").sortable().build(), Field.text("title").sortable().build());
		sync.ftCreate("releases", options, fields.toArray(Field[]::new));
		assertEquals(fields.size(), RedisModulesUtils.indexInfo(sync.ftInfo("releases")).getFields().size());
	}

	@Test
	@SuppressWarnings("unchecked")
	void ftCreateVector() throws Exception {
		Beers.populateIndex(connection);
		RedisModulesCommands<String, String> sync = connection.sync();
		String vectorIndex = "vectorTestIndex";
		sync.ftCreate(vectorIndex, CreateOptions.<String, String>builder().prefix("vectortest:").build(),
				VectorField.name("fields1").algorithm(SearchCommandKeyword.FLAT)
						.vectorType(SearchCommandKeyword.FLOAT32).distanceMetric(SearchCommandKeyword.COSINE).dim(5)
						.build());

		assertEquals(vectorIndex, RedisModulesUtils.indexInfo(sync.ftInfo(vectorIndex)).getIndexName());
	}

	@SuppressWarnings("unchecked")
	@Test
	void ftCreateTemporaryIndex() throws Exception {
		populateIndex(connection);
		RedisModulesCommands<String, String> sync = connection.sync();
		String tempIndex = "temporaryIndex";
		sync.ftCreate(tempIndex, CreateOptions.<String, String>builder().temporary(1L).build(),
				Field.text("field1").build());
		assertEquals(tempIndex, RedisModulesUtils.indexInfo(sync.ftInfo(tempIndex)).getIndexName());
		Awaitility.await().until(() -> !sync.ftList().contains(tempIndex));
	}

	@Test
	void ftAlterIndex() throws Exception {
		populateIndex(connection);
		RedisModulesCommands<String, String> sync = connection.sync();
		Map<String, Object> indexInfo = toMap(sync.ftInfo(INDEX));
		assertEquals(INDEX, indexInfo.get("index_name"));
		sync.ftAlter(INDEX, Field.tag("newField").build());
		Map<String, String> doc = mapOf("newField", "value1");
		sync.hmset("beer:newDoc", doc);
		SearchResults<String, String> results = sync.ftSearch(INDEX, "@newField:{value1}");
		assertEquals(1, results.getCount());
		assertEquals(doc.get("newField"), results.get(0).get("newField"));
	}

	@Test
	void ftDropindexDeleteDocs() throws Exception {
		populateIndex(connection);
		RedisModulesCommands<String, String> sync = connection.sync();
		sync.ftDropindexDeleteDocs(INDEX);
		Awaitility.await().until(() -> sync.ftList().isEmpty());
		Assertions.assertThrows(RedisCommandExecutionException.class, () -> sync.ftInfo(INDEX));
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

	@SuppressWarnings("unchecked")
	@Test
	void ftList() throws ExecutionException, InterruptedException {
		RedisModulesCommands<String, String> sync = connection.sync();
		sync.flushall();
		Set<String> indexNames = new HashSet<>(Arrays.asList("index1", "index2", "index3"));
		for (String indexName : indexNames) {
			sync.ftCreate(indexName, Field.text("field1").sortable().build());
		}
		assertEquals(indexNames, new HashSet<>(sync.ftList()));
		assertEquals(indexNames, new HashSet<>(connection.async().ftList().get()));
		assertEquals(indexNames, new HashSet<>(connection.reactive().ftList().collectList().block()));
	}

	@Test
	void ftSearchOptions() throws IOException {
		populateIndex(connection);
		RedisModulesCommands<String, String> sync = connection.sync();
		SearchOptions<String, String> options = SearchOptions.<String, String>builder().withPayloads(true)
				.noStopWords(true).limit(10, 100).withScores(true)
				.highlight(Highlight.<String, String>builder().field(NAME).tags("<TAG>", "</TAG>").build())
				.language(Language.ENGLISH).noContent(false).timeout(Duration.ofSeconds(10)).param("param1", "value")
				.dialect(2).sortBy(SearchOptions.SortBy.asc(NAME)).verbatim(false).withSortKeys().returnField(NAME)
				.returnField(STYLE).build();
		SearchResults<String, String> results = sync.ftSearch(INDEX, "pale", options);
		assertEquals(74, results.getCount());
		Document<String, String> doc1 = results.get(0);
		assertNotNull(doc1.get(NAME));
		assertNotNull(doc1.get(STYLE));
		assertNull(abv(doc1));

	}

	private void assertSearch(String query, SearchOptions<String, String> options, long expectedCount,
			String... expectedAbv) {
		SearchResults<String, String> results = connection.sync().ftSearch(INDEX, query, options);
		assertEquals(expectedCount, results.getCount());
		Document<String, String> doc1 = results.get(0);
		assertNotNull(doc1.get(NAME));
		assertNotNull(doc1.get(STYLE));
		if (expectedAbv.length > 0) {
			assertTrue(Arrays.asList(expectedAbv).contains(doc1.get(ABV)));
		}
	}

	@Test
	void ftSearch() throws Exception {
		populateIndex(connection);
		RedisModulesCommands<String, String> sync = connection.sync();
		SearchResults<String, String> results = sync.ftSearch(INDEX, "German");
		assertEquals(3, results.getCount());
		results = sync.ftSearch(INDEX, "Hefeweizen", SearchOptions.<String, String>builder().noContent(true).build());
		assertEquals(10, results.size());
		assertTrue(results.get(0).getId().startsWith("beer:"));
		results = sync.ftSearch(INDEX, "Hefeweizen", SearchOptions.<String, String>builder().withScores(true).build());
		assertEquals(10, results.size());
		assertTrue(results.get(0).getScore() > 0);
		results = sync.ftSearch(INDEX, "Hefeweizen", SearchOptions.<String, String>builder().withScores(true)
				.noContent(true).limit(new Limit(0, 100)).build());
		assertEquals(14, results.getCount());
		assertEquals(14, results.size());
		assertTrue(results.get(0).getId().startsWith(PREFIX));
		assertTrue(results.get(0).getScore() > 0);

		results = sync.ftSearch(INDEX, "pale", SearchOptions.<String, String>builder().withPayloads(true).build());
		assertEquals(74, results.getCount());
		Document<String, String> result1 = results.get(0);
		assertNotNull(result1.get(NAME));
		assertEquals(result1.get(DESCRIPTION), result1.getPayload());
		assertEquals(sync.hget(result1.getId(), DESCRIPTION), result1.getPayload());

		assertSearch("pale", SearchOptions.<String, String>builder().returnField(NAME).returnField(STYLE).build(), 74);
		assertSearch("pale",
				SearchOptions.<String, String>builder().returnField(NAME).returnField(STYLE).returnField("").build(),
				74);
		assertSearch("*", SearchOptions.<String, String>builder().inKeys("beer:728", "beer:803").build(), 2,
				"5.800000190734863", "8");
		assertSearch("wise", SearchOptions.<String, String>builder().inField(NAME).build(), 1, "5.900000095367432");
	}

	@SuppressWarnings("unchecked")
	@Test
	void ftSearchTags() throws InterruptedException, ExecutionException, IOException {
		int count = populateIndex(connection);
		RedisModulesCommands<String, String> sync = connection.sync();
		String term = "pale";
		String query = "@style:" + term;
		Tags<String> tags = new Tags<>("<b>", "</b>");
		SearchResults<String, String> results = sync.ftSearch(INDEX, query, SearchOptions.<String, String>builder()
				.highlight(SearchOptions.Highlight.<String, String>builder().build()).build());
		for (Document<String, String> result : results) {
			assertTrue(isHighlighted(result, STYLE, tags, term));
		}
		results = sync.ftSearch(INDEX, query, SearchOptions.<String, String>builder()
				.highlight(SearchOptions.Highlight.<String, String>builder().field(NAME).build()).build());
		for (Document<String, String> result : results) {
			assertFalse(isHighlighted(result, STYLE, tags, term));
		}
		tags = new Tags<>("[start]", "[end]");
		results = sync.ftSearch(INDEX, query, SearchOptions.<String, String>builder()
				.highlight(SearchOptions.Highlight.<String, String>builder().field(STYLE).tags(tags).build()).build());
		for (Document<String, String> result : results) {
			assertTrue(isHighlighted(result, STYLE, tags, term));
		}

		results = connection.reactive().ftSearch(INDEX, "pale",
				SearchOptions.<String, String>builder().limit(Limit.offset(10).num(20)).build()).block();
		assertEquals(74, results.getCount());
		Document<String, String> result1 = results.get(0);
		assertNotNull(result1.get(NAME));
		assertNotNull(result1.get(STYLE));
		assertNotNull(abv(result1));

		results = sync.ftSearch(INDEX, "pail");
		assertEquals(17, results.getCount());

		results = sync.ftSearch(INDEX, "*", SearchOptions.<String, String>builder().limit(new Limit(0, 0)).build());
		assertEquals(count, results.getCount());

		String index = "escapeTagTestIdx";
		String idField = "id";
		connection.async().ftCreate(index, Field.tag(idField).build()).get();
		Map<String, String> doc1 = new HashMap<>();
		doc1.put(idField, "chris@blah.org,User1#test.org,usersdfl@example.com");
		connection.async().hmset("doc1", doc1).get();
		results = connection.async().ftSearch(index, "@id:{" + RedisModulesUtils.escapeTag("User1#test.org") + "}")
				.get();
		assertEquals(1, results.size());
		double minABV = .18;
		double maxABV = 10;
		SearchResults<String, String> filterResults = sync.ftSearch(INDEX, "*", SearchOptions.<String, String>builder()
				.filter(SearchOptions.NumericFilter.<String, String>field(ABV).min(minABV).max(maxABV)).build());
		assertEquals(10, filterResults.size());
		for (Document<String, String> document : filterResults) {
			Double abv = abv(document);
			Assertions.assertTrue(abv >= minABV);
			Assertions.assertTrue(abv <= maxABV);
		}
	}

	private boolean isHighlighted(Document<String, String> result, String fieldName, Tags<String> tags, String string) {
		return result.get(fieldName).toLowerCase().contains(tags.getOpen() + string + tags.getClose());
	}

	@Test
	void sugget() throws IOException {
		createBeerSuggestions();
		RedisModulesCommands<String, String> sync = connection.sync();
		RedisModulesReactiveCommands<String, String> reactive = connection.reactive();
		assertEquals(1, sync.ftSugget(SUGINDEX, "Ame").size());
		assertEquals(1, reactive.ftSugget(SUGINDEX, "Ame").collectList().block().size());
		SuggetOptions options = SuggetOptions.builder().max(1000L).build();
		assertEquals(1, sync.ftSugget(SUGINDEX, "Ame", options).size());
		assertEquals(1, reactive.ftSugget(SUGINDEX, "Ame", options).collectList().block().size());
		Consumer<List<Suggestion<String>>> withScores = results -> {
			assertEquals(1, results.size());
			assertEquals("American Pale Ale", results.get(0).getString());
			assertEquals(0.2773500978946686, results.get(0).getScore(), .01);
		};
		SuggetOptions withScoresOptions = SuggetOptions.builder().max(1000L).withScores(true).build();
		withScores.accept(sync.ftSugget(SUGINDEX, "Ameri", withScoresOptions));
		withScores.accept(reactive.ftSugget(SUGINDEX, "Ameri", withScoresOptions).collectList().block());
		assertEquals(410, sync.ftSuglen(SUGINDEX));
		assertTrue(sync.ftSugdel(SUGINDEX, "American Pale Ale"));
		assertFalse(reactive.ftSugdel(SUGINDEX, "Thunderstorm").block());
	}

	private Map<String, Map<String, Object>> populateBeers() throws IOException {
		int count = populateIndex(connection);
		MappingIterator<Map<String, Object>> beers = mapIterator();
		Map<String, Map<String, Object>> beerMap = new HashMap<>();
		while (beers.hasNext()) {
			Map<String, Object> beer = beers.next();
			beerMap.put((String) beer.get(ID), beer);
		}
		Assertions.assertEquals(count, beerMap.size());
		return beerMap;
	}

	@Test
	void ftAggregateLoad() throws Exception {
		Map<String, Map<String, Object>> beers = populateBeers();
		Consumer<AggregateResults<String>> loadAsserts = results -> {
			assertEquals(1, results.getCount());
			assertEquals(beers.size(), results.size());
			for (Map<String, Object> result : results) {
				String id = (String) result.get(ID);
				Map<String, Object> beer = beers.get(id);
				assertEquals(((String) beer.get(NAME)).toLowerCase(), ((String) result.get(NAME)).toLowerCase());
				String style = (String) beer.get("style");
				if (style != null) {
					assertEquals(style.toLowerCase(), ((String) result.get("style")).toLowerCase());
				}
			}
		};
		RedisModulesCommands<String, String> sync = connection.sync();
		RedisModulesReactiveCommands<String, String> reactive = connection.reactive();
		AggregateOptions<String, String> options = AggregateOptions.<String, String>builder().load(ID)
				.load(Load.identifier(NAME).build()).load(Load.identifier(STYLE).as("style").build()).build();
		loadAsserts.accept(sync.ftAggregate(INDEX, "*", options));
		loadAsserts.accept(reactive.ftAggregate(INDEX, "*", options).block());
	}

	@Test
	void ftAggregateGroupSortLimit() throws Exception {
		populateBeers();
		Consumer<AggregateResults<String>> asserts = results -> {
			assertEquals(36, results.getCount());
			List<Double> abvs = results.stream().map(r -> Double.parseDouble((String) r.get(ABV)))
					.collect(Collectors.toList());
			assertTrue(abvs.get(0) > abvs.get(abvs.size() - 1));
			assertEquals(20, results.size());
		};
		AggregateOptions<String, String> options = AggregateOptions
				.<String, String>operation(Group.by(STYLE).reducer(Avg.property(ABV).as(ABV).build()).build())
				.operation(Sort.by(Sort.Property.desc(ABV)).build()).operation(Limit.offset(0).num(20)).build();
		asserts.accept(connection.sync().ftAggregate(INDEX, "*", options));
		asserts.accept(connection.reactive().ftAggregate(INDEX, "*", options).block());
	}

	@Test
	void ftAggregateSort() throws Exception {
		populateBeers();
		Consumer<AggregateResults<String>> asserts = results -> {
			assertEquals(467, results.getCount());
			assertEquals(10, results.size());
			List<Double> abvs = results.stream().map(this::abv).collect(Collectors.toList());
			assertTrue(abvs.get(0) > abvs.get(abvs.size() - 1));
		};
		AggregateOptions<String, String> options = AggregateOptions
				.<String, String>operation(Sort.by(Sort.Property.desc(ABV)).by(Property.asc(IBU)).build()).build();
		asserts.accept(connection.sync().ftAggregate(INDEX, "*", options));
		asserts.accept(connection.reactive().ftAggregate(INDEX, "*", options).block());
	}

	private Double abv(Map<String, ?> map) {
		Object value = map.get(ABV);
		if (value == null) {
			return null;
		}
		return Double.parseDouble((String) value);
	}

	@Test
	void ftAggregateGroupNone() throws Exception {
		populateBeers();
		Consumer<AggregateResults<String>> asserts = results -> {
			assertEquals(1, results.getCount());
			assertEquals(1, results.size());
			Double maxAbv = abv(results.get(0));
			assertEquals(16, maxAbv, 0.1);
		};

		AggregateOptions<String, String> options = AggregateOptions
				.<String, String>operation(Group.by().reducer(Max.property(ABV).as(ABV).build()).build()).build();
		asserts.accept(connection.sync().ftAggregate(INDEX, "*", options));
		asserts.accept(connection.reactive().ftAggregate(INDEX, "*", options).block());

	}

	@SuppressWarnings("unchecked")
	@Test
	void ftAggregateGroupToList() throws Exception {
		populateBeers();
		Consumer<AggregateResults<String>> asserts = results -> {
			assertEquals(36, results.getCount());
			Map<String, Object> doc = results.get(1);
			Assumptions.assumeTrue(doc != null);
			Assumptions.assumeTrue(doc.get(STYLE) != null);
			String style = ((String) doc.get(STYLE)).toLowerCase();
			assertTrue(style.equals("bamberg-style bock rauchbier") || style.equals("south german-style hefeweizen"));
			int nameCount = ((List<String>) results.get(1).get("names")).size();
			assertEquals(21, nameCount);
		};
		Group group = Group.by(STYLE).reducer(ToList.property(NAME).as("names").build()).reducer(Count.as("count"))
				.build();
		AggregateOptions<String, String> options = AggregateOptions.<String, String>operation(group)
				.operation(Limit.offset(0).num(2)).build();
		asserts.accept(connection.sync().ftAggregate(INDEX, "*", options));
		asserts.accept(connection.reactive().ftAggregate(INDEX, "*", options).block());
	}

	@Test
	void ftAggregateCursor() throws Exception {
		populateBeers();
		Consumer<AggregateWithCursorResults<String>> cursorTests = cursorResults -> {
			assertEquals(1, cursorResults.getCount());
			assertEquals(10, cursorResults.size());
			assertTrue(((String) cursorResults.get(9).get(ABV)).length() > 0);
		};
		AggregateOptions<String, String> cursorOptions = AggregateOptions.<String, String>builder().load(ID).load(NAME)
				.load(ABV).build();
		AggregateWithCursorResults<String> cursorResults = connection.sync().ftAggregate(INDEX, "*",
				CursorOptions.builder().count(10).build(), cursorOptions);
		cursorTests.accept(cursorResults);
		cursorTests.accept(connection.reactive()
				.ftAggregate(INDEX, "*", CursorOptions.builder().count(10).build(), cursorOptions).block());
		cursorResults = connection.sync().ftCursorRead(INDEX, cursorResults.getCursor(), 400);
		assertEquals(400, cursorResults.size());
		String deleteStatus = connection.sync().ftCursorDelete(INDEX, cursorResults.getCursor());
		assertEquals("OK", deleteStatus);
	}

	@Test
	void ftAlias() throws Exception {
		populateIndex(connection);

		// SYNC

		String alias = "alias123";

		RedisModulesCommands<String, String> sync = connection.sync();
		sync.ftAliasadd(alias, INDEX);
		SearchResults<String, String> results = sync.ftSearch(alias, "*");
		assertTrue(results.size() > 0);

		String newAlias = "alias456";
		sync.ftAliasupdate(newAlias, INDEX);
		assertTrue(sync.ftSearch(newAlias, "*").size() > 0);

		sync.ftAliasdel(newAlias);
		Assertions.assertThrows(RedisCommandExecutionException.class, () -> sync.ftSearch(newAlias, "*"),
				"no such index");

		sync.ftAliasdel(alias);
		RedisModulesAsyncCommands<String, String> async = connection.async();
		// ASYNC
		async.ftAliasadd(alias, INDEX).get();
		results = async.ftSearch(alias, "*").get();
		assertTrue(results.size() > 0);

		async.ftAliasupdate(newAlias, INDEX).get();
		assertTrue(async.ftSearch(newAlias, "*").get().size() > 0);

		async.ftAliasdel(newAlias).get();
		Assertions.assertThrows(ExecutionException.class, () -> async.ftSearch(newAlias, "*").get(), "no such index");

		sync.ftAliasdel(alias);

		RedisModulesReactiveCommands<String, String> reactive = connection.reactive();
		// REACTIVE
		reactive.ftAliasadd(alias, INDEX).block();
		results = reactive.ftSearch(alias, "*").block();
		assertTrue(results.size() > 0);

		reactive.ftAliasupdate(newAlias, INDEX).block();
		results = reactive.ftSearch(newAlias, "*").block();
		Assertions.assertFalse(results.isEmpty());

		reactive.ftAliasdel(newAlias).block();
		Mono<SearchResults<String, String>> searchResults = reactive.ftSearch(newAlias, "*");
		Assertions.assertThrows(RedisCommandExecutionException.class, () -> searchResults.block(), "no such index");
	}

	@Test
	void ftInfo() throws Exception {
		int count = populateIndex(connection);
		IndexInfo info = RedisModulesUtils.indexInfo(connection.async().ftInfo(INDEX).get());
		assertEquals(count, info.getNumDocs());
		List<Field<String>> fields = info.getFields();
		TextField<String> descriptionField = (TextField<String>) fields.get(5);
		assertEquals(DESCRIPTION, descriptionField.getName());
		Assertions.assertFalse(descriptionField.isNoIndex());
		Assertions.assertTrue(descriptionField.isNoStem());
		Assertions.assertFalse(descriptionField.isSortable());
		TagField<String> styleField = (TagField<String>) fields.get(2);
		assertEquals(STYLE, styleField.getName());
		Assertions.assertTrue(styleField.isSortable());
		assertEquals(',', styleField.getSeparator().get());
	}

	@Test
	void geoLocation() {
		double longitude = -118.753604;
		double latitude = 34.027201;
		String locationString = "-118.753604,34.027201";
		GeoLocation location = GeoLocation.of(locationString);
		Assertions.assertEquals(longitude, location.getLongitude());
		Assertions.assertEquals(latitude, location.getLatitude());
		Assertions.assertEquals(locationString,
				GeoLocation.toString(String.valueOf(longitude), String.valueOf(latitude)));
	}

	@SuppressWarnings("unchecked")
	@Test
	void ftSearchJSON() throws Exception {
		Iterator<JsonNode> iterator = jsonNodeIterator();
		String index = "beers";
		TagField<String> idField = Field.tag(jsonField(ID)).as(ID).build();
		TextField<String> nameField = Field.text(jsonField(NAME)).as(NAME).build();
		TextField<String> styleField = Field.text(jsonField(STYLE)).build();
		connection.sync().ftCreate(index, CreateOptions.<String, String>builder().on(DataType.JSON).build(), idField,
				nameField, styleField);
		IndexInfo info = RedisModulesUtils.indexInfo(connection.sync().ftInfo(index));
		Assertions.assertEquals(3, info.getFields().size());
		Assertions.assertEquals(idField.getAs(), info.getFields().get(0).getAs());
		Assertions.assertEquals(styleField.getName(), info.getFields().get(2).getAs().get());
		while (iterator.hasNext()) {
			JsonNode beer = iterator.next();
			connection.sync().jsonSet("beer:" + beer.get(ID).asText(), JsonPath.ROOT_PATH,
					connection.sync().getJsonParser().createJsonValue(beer.toString()));
		}
		SearchResults<String, String> results = connection.sync().ftSearch(index, "@" + NAME + ":Creek");
		Assertions.assertEquals(1, results.getCount());
	}

	@SuppressWarnings("unchecked")
	@Test
	void ftSearchJSONWithNullValues() throws Exception {
		String index = "accounts";
		TagField<String> idField = Field.tag(jsonField(ID)).as(ID).build();
		TagField<String> nameField = Field.tag(jsonField(NAME)).as(NAME).build();
		TagField<String> styleField = Field.tag(jsonField(STYLE)).as(STYLE).build();
		connection.sync().ftCreate(index,
				CreateOptions.<String, String>builder().on(DataType.JSON).prefix("account:").build(), idField,
				nameField, styleField);
		connection.sync().jsonSet("account:1", JsonPath.ROOT_PATH, connection.sync().getJsonParser()
				.createJsonValue("{\"id\": \"1\", \"name\": null, \"style_name\": \"123\"}"));
		SearchResults<String, String> results = connection.sync().ftSearch(index, "*",
				SearchOptions.<String, String>builder().returnFields("id", "name", "style_name").build());
		Assertions.assertEquals(1, results.getCount());
	}

	private String jsonField(String name) {
		return "$." + name;
	}

	@SuppressWarnings("unchecked")
	@Test
	void ftInfoFields() {
		String index = "indexFields";
		TagField<String> idField = Field.tag(jsonField(ID)).as(ID).separator('-').build();
		TextField<String> nameField = Field.text(jsonField(NAME)).as(NAME).noIndex().noStem().unNormalizedForm()
				.weight(2).build();
		String styleFieldName = jsonField(STYLE);
		TextField<String> styleField = Field.text(styleFieldName).as(styleFieldName).weight(1).build();
		connection.sync().ftCreate(index, CreateOptions.<String, String>builder().on(DataType.JSON).build(), idField,
				nameField, styleField);
		IndexInfo info = RedisModulesUtils.indexInfo(connection.sync().ftInfo(index));
		Assertions.assertEquals(idField, info.getFields().get(0));
		Field<String> actualNameField = info.getFields().get(1);
		// Workaround for older RediSearch versions (Redis Enterprise tests)
		actualNameField.setUnNormalizedForm(true);
		Assertions.assertEquals(nameField, actualNameField);
		Assertions.assertEquals(styleField, info.getFields().get(2));
	}

	@SuppressWarnings("unchecked")
	@Test
	void ftInfoOptions() {
		RedisModulesCommands<String, String> commands = connection.sync();
		String index = "indexWithOptions";
		CreateOptions<String, String> createOptions = CreateOptions.<String, String>builder().on(DataType.JSON)
				.prefixes("prefix1", "prefix2").filter("@indexName==\"myindexname\"").defaultLanguage(Language.CHINESE)
				.languageField("languageField").defaultScore(.5).scoreField("scoreField").payloadField("payloadField")
				.maxTextFields(true).noOffsets(true).noHL(true).noFields(true).noFreqs(true).build();
		commands.ftCreate(index, createOptions, Field.tag("id").build(), Field.numeric("scoreField").build());
		IndexInfo info = RedisModulesUtils.indexInfo(commands.ftInfo(index));
		CreateOptions<String, String> actual = info.getIndexOptions();
		actual.setNoHL(true); // Hack to get around Redisearch version differences between Enterprise and OSS
		Assertions.assertEquals(createOptions, actual);
	}

	@Test
	void ftTagVals() throws Exception {
		populateIndex(connection);
		Set<String> TAG_VALS = new HashSet<>(Arrays.asList(
				"american-style brown ale, traditional german-style bock, german-style schwarzbier, old ale, american-style india pale ale, german-style oktoberfest, other belgian-style ales, american-style stout, winter warmer, belgian-style tripel, american-style lager, belgian-style dubbel, porter, american-style barley wine ale, belgian-style fruit lambic, scottish-style light ale, south german-style hefeweizen, imperial or double india pale ale, golden or blonde ale, belgian-style quadrupel, american-style imperial stout, belgian-style pale strong ale, english-style pale mild ale, american-style pale ale, irish-style red ale, dark american-belgo-style ale, light american wheat ale or lager, german-style pilsener, american-style amber/red ale, scotch ale, german-style doppelbock, extra special bitter, south german-style weizenbock, english-style india pale ale, belgian-style pale ale, french & belgian-style saison"
						.split(", ")));
		HashSet<String> actual = new HashSet<>(connection.sync().ftTagvals(INDEX, STYLE));
		assertEquals(TAG_VALS, actual);
		assertEquals(TAG_VALS, new HashSet<>(connection.reactive().ftTagvals(INDEX, STYLE).collectList().block()));
	}

	@SuppressWarnings("unchecked")
	@Test
	void ftAggregateEmptyToListReducer() {
		RedisModulesCommands<String, String> sync = connection.sync();
		// FT.CREATE idx ON HASH PREFIX 1 my_prefix: SCHEMA category TAG SORTABLE color
		// TAG SORTABLE size TAG SORTABLE
		sync.ftCreate("idx", CreateOptions.<String, String>builder().prefix("my_prefix:").build(),
				Field.tag("category").sortable().build(), Field.tag("color").sortable().build(),
				Field.tag("size").sortable().build());
		Map<String, String> doc1 = mapOf("category", "31", "color", "red");
		sync.hset("my_prefix:1", doc1);
		AggregateOptions<String, String> aggregateOptions = AggregateOptions.<String, String>operation(Group
				.by("category")
				.reducers(ToList.property("color").as("color").build(), ToList.property("size").as("size").build())
				.build()).build();
		AggregateResults<String> results = sync.ftAggregate("idx", "@color:{red|blue}", aggregateOptions);
		assertEquals(1, results.size());
		Map<String, Object> expectedResult = new HashMap<>();
		expectedResult.put("category", "31");
		expectedResult.put("color", Collections.singletonList("red"));
		expectedResult.put("size", Collections.emptyList());
		assertEquals(expectedResult, results.get(0));
	}

	@Test
	void ftDictadd() {
		String[] DICT_TERMS = new String[] { "beer", "ale", "brew", "brewski" };
		RedisModulesCommands<String, String> sync = connection.sync();
		assertEquals(DICT_TERMS.length, sync.ftDictadd("beers", DICT_TERMS));
		assertEquals(new HashSet<>(Arrays.asList(DICT_TERMS)), new HashSet<>(sync.ftDictdump("beers")));
		assertEquals(1, sync.ftDictdel("beers", "brew"));
		List<String> beerDict = new ArrayList<>();
		Collections.addAll(beerDict, DICT_TERMS);
		beerDict.remove("brew");
		assertEquals(new HashSet<>(beerDict), new HashSet<>(sync.ftDictdump("beers")));
		connection.sync().flushall();
		RedisModulesReactiveCommands<String, String> reactive = connection.reactive();
		assertEquals(DICT_TERMS.length, reactive.ftDictadd("beers", DICT_TERMS).block());
		assertEquals(new HashSet<>(Arrays.asList(DICT_TERMS)),
				new HashSet<>(reactive.ftDictdump("beers").collectList().block()));
		assertEquals(1, reactive.ftDictdel("beers", "brew").block());
		beerDict = new ArrayList<>();
		Collections.addAll(beerDict, DICT_TERMS);
		beerDict.remove("brew");
		assertEquals(new HashSet<>(beerDict), new HashSet<>(reactive.ftDictdump("beers").collectList().block()));
	}

	@SuppressWarnings("unchecked")
	@Test
	void tsAdd() {
		RedisTimeSeriesCommands<String, String> ts = connection.sync();
		// TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
		// TS.ADD temperature:3:11 1548149181 30
		Long add1 = ts.tsAdd(TS_KEY, Sample.of(TIMESTAMP_1, VALUE_1),
				AddOptions.<String, String>builder().retentionPeriod(6000)
						.labels(KeyValue.just(LABEL_SENSOR_ID, SENSOR_ID), KeyValue.just(LABEL_AREA_ID, AREA_ID))
						.build());
		assertEquals(TIMESTAMP_1, add1);
		Sample sample = ts.tsGet(TS_KEY);
		assertEquals(TIMESTAMP_1, sample.getTimestamp());
		// TS.ADD temperature:3:11 1548149191 42
		Long add2 = ts.tsAdd(TS_KEY, Sample.of(TIMESTAMP_2, VALUE_2));
		assertEquals(TIMESTAMP_2, add2);
	}

	@Test
	void tsRange() {
		RedisTimeSeriesCommands<String, String> ts = connection.sync();
		populate(ts);
		assertRange(ts.tsRange(TS_KEY, TimeRange.builder().from(TIMESTAMP_1 - 10).to(TIMESTAMP_2 + 10).build(),
				RangeOptions.builder()
						.aggregation(
								Aggregation.aggregator(Aggregator.AVG).bucketDuration(Duration.ofMillis(5)).build())
						.build()));
		assertRange(ts.tsRange(TS_KEY, TimeRange.unbounded(),
				RangeOptions.builder()
						.aggregation(
								Aggregation.aggregator(Aggregator.AVG).bucketDuration(Duration.ofMillis(5)).build())
						.build()));
		assertRange(ts.tsRange(TS_KEY, TimeRange.from(TIMESTAMP_1 - 10).build(),
				RangeOptions.builder()
						.aggregation(
								Aggregation.aggregator(Aggregator.AVG).bucketDuration(Duration.ofMillis(5)).build())
						.build()));
		assertRange(ts.tsRange(TS_KEY, TimeRange.to(TIMESTAMP_2 + 10).build(),
				RangeOptions.builder()
						.aggregation(
								Aggregation.aggregator(Aggregator.AVG).bucketDuration(Duration.ofMillis(5)).build())
						.build()));
	}

	private void assertRange(List<Sample> results) {
		assertEquals(2, results.size());
		assertEquals(1548149180, results.get(0).getTimestamp());
		assertEquals(VALUE_1, results.get(0).getValue());
		assertEquals(1548149190, results.get(1).getTimestamp());
		assertEquals(VALUE_2, results.get(1).getValue());
	}

	@SuppressWarnings("unchecked")
	protected void populate(RedisTimeSeriesCommands<String, String> ts) {
		// TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
		// TS.ADD temperature:3:11 1548149181 30
		ts.tsAdd(TS_KEY, Sample.of(TIMESTAMP_1, VALUE_1), AddOptions.<String, String>builder().retentionPeriod(6000)
				.labels(KeyValue.just(LABEL_SENSOR_ID, SENSOR_ID), KeyValue.just(LABEL_AREA_ID, AREA_ID)).build());
		// TS.ADD temperature:3:11 1548149191 42
		ts.tsAdd(TS_KEY, Sample.of(TIMESTAMP_2, VALUE_2));

		ts.tsAdd(TS_KEY_2, Sample.of(TIMESTAMP_1, VALUE_1), AddOptions.<String, String>builder().retentionPeriod(6000)
				.labels(KeyValue.just(LABEL_SENSOR_ID, SENSOR_ID), KeyValue.just(LABEL_AREA_ID, AREA_ID_2)).build());
		ts.tsAdd(TS_KEY_2, Sample.of(TIMESTAMP_2, VALUE_2));
	}

	@Test
	void tsGet() {
		RedisTimeSeriesCommands<String, String> ts = connection.sync();
		populate(ts);
		Sample result = ts.tsGet(TS_KEY);
		Assertions.assertEquals(TIMESTAMP_2, result.getTimestamp());
		Assertions.assertEquals(VALUE_2, result.getValue());
		ts.tsCreate("ts:empty", com.redis.lettucemod.timeseries.CreateOptions.<String, String>builder().build());
		Assertions.assertNull(ts.tsGet("ts:empty"));
	}

	@Test
	void utilsIndexInfo() {
		Assertions.assertTrue(RedisModulesUtils.indexInfo(() -> connection.sync().ftInfo("wweriwjer")).isEmpty());
	}

	protected void assertJSONEquals(String expected, String actual)
			throws JsonMappingException, JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		assertEquals(mapper.readTree(expected), mapper.readTree(actual));
	}

	@Test
	void bfBasic() {
		String key = "test:bfBasic";
		String keyInsert = "test:bfInsert";
		RedisBloomCommands<String, String> bf = connection.sync();
		connection.sync().unlink(key, keyInsert);
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
		BloomFilterInsertOptions options = BloomFilterInsertOptions.builder().capacity(10000).error(.01).expansion(2)
				.build();
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
		connection.sync().unlink(key, keyInsert);

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
		BloomFilterInsertOptions options = BloomFilterInsertOptions.builder().capacity(10000).error(.01).expansion(2)
				.build();
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
		connection.sync().unlink(key1, key2);
		RedisBloomCommands<String, String> cf = connection.sync();
		assertEquals("OK", cf.cfReserve(key1, 1000L));
		assertTrue(cf.cfAdd(key1, "test"));
		assertFalse(cf.cfAddNx(key1, "test"));
		assertEquals(1, cf.cfCount(key1, "test"));
		assertTrue(cf.cfDel(key1, "test"));
		CuckooFilterInsertOptions options = CuckooFilterInsertOptions.builder().capacity(10000L).noCreate(false)
				.build();
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
		connection.sync().unlink(key1, key2);
		RedisBloomReactiveCommands<String, String> cf = connection.reactive();
		assertEquals("OK", cf.cfReserve(key1, 1000L).block());
		assertEquals(Boolean.TRUE, cf.cfAdd(key1, "test").block());
		assertNotEquals(Boolean.TRUE, cf.cfAddNx(key1, "test").block());
		assertEquals(1, cf.cfCount(key1, "test").block());
		assertEquals(Boolean.TRUE, cf.cfDel(key1, "test").block());
		CuckooFilterInsertOptions options = CuckooFilterInsertOptions.builder().capacity(10000L).noCreate(false)
				.build();
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

		connection.sync().unlink(key1, key2);
		RedisBloomCommands<String, String> cms = connection.sync();

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

		connection.sync().unlink(key1, key2);
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
		connection.sync().unlink(key1, key2);
		RedisBloomCommands<String, String> topK = connection.sync();
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
		connection.sync().unlink(key1, key2);
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
		connection.sync().unlink(key);
		assertEquals("OK", connection.sync().tDigestCreate(key));
		double[] quantiles = { 0.1, 0.2, 0.3 };
		List<Double> res = connection.sync().tDigestQuantile(key, quantiles);
		assertEquals(Double.NaN, res.get(0));
		assertEquals(Double.NaN, res.get(1));
		assertEquals(Double.NaN, res.get(2));

		res = connection.sync().tDigestByRank(key, 4, 5, 6);
		assertEquals(Double.NaN, res.get(0));
		assertEquals(Double.NaN, res.get(1));
		assertEquals(Double.NaN, res.get(2));

		res = connection.sync().tDigestByRevRank(key, 4, 5, 6);
		assertEquals(Double.NaN, res.get(0));
		assertEquals(Double.NaN, res.get(1));
		assertEquals(Double.NaN, res.get(2));

		res = connection.sync().tDigestCdf(key, .4, .5);
		assertEquals(Double.NaN, res.get(0));
		assertEquals(Double.NaN, res.get(1));

		double singleResult = connection.sync().tDigestMin(key);
		assertEquals(Double.NaN, singleResult);

		singleResult = connection.sync().tDigestMax(key);
		assertEquals(Double.NaN, singleResult);

		singleResult = connection.sync().tDigestTrimmedMean(key, 0, 1);
		assertEquals(Double.NaN, singleResult);
	}

	@Test
	void tDigestInf() {
		String key = "tdigest:inf";
		connection.sync().unlink(key);
		RedisBloomCommands<String, String> tDigest = connection.sync();
		assertEquals("OK", tDigest.tDigestCreate(key, 1000));
		assertEquals("OK", tDigest.tDigestAdd(key, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
		assertEquals(Double.POSITIVE_INFINITY, tDigest.tDigestByRank(key, 25).get(0));
		assertEquals(Double.NEGATIVE_INFINITY, tDigest.tDigestByRevRank(key, 25).get(0));
	}

	@Test
	void tDigest() {
		String key = "tdigest:1";
		connection.sync().unlink(key);
		RedisBloomCommands<String, String> tDigest = connection.sync();
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
		connection.sync().unlink(key);
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
}
