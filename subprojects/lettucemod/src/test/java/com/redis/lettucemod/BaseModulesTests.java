package com.redis.lettucemod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.reactive.RedisModulesReactiveCommands;
import com.redis.lettucemod.api.sync.RedisGearsCommands;
import com.redis.lettucemod.api.sync.RedisJSONCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.api.sync.RedisTimeSeriesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.SetMode;
import com.redis.lettucemod.json.Slice;
import com.redis.lettucemod.output.ExecutionResults;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateOptions.Load;
import com.redis.lettucemod.search.AggregateResults;
import com.redis.lettucemod.search.AggregateWithCursorResults;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.CreateOptions.DataType;
import com.redis.lettucemod.search.CursorOptions;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.Field;
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
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.Aggregator;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.lettucemod.util.RedisURIBuilder;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.AclSetuserArgs;
import io.lettuce.core.KeyValue;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;
import reactor.core.publisher.Mono;

@Testcontainers
@TestInstance(Lifecycle.PER_CLASS)
abstract class BaseModulesTests {

	private static final Logger log = LoggerFactory.getLogger(BaseModulesTests.class);
	protected static final String SUGINDEX = "beersSug";

	protected static StatefulRedisModulesConnection<String, String> connection;
	private static AbstractRedisClient client;

	@BeforeAll
	void setup() {
		log.info("Setting up test instance");
		RedisServer server = getRedisServer();
		server.start();
		RedisURI uri = RedisURI.create(server.getRedisURI());
		uri.setTimeout(Duration.ofMillis(300));
		client = server.isCluster() ? RedisModulesClusterClient.create(uri) : RedisModulesClient.create(uri);
		connection = RedisModulesUtils.connection(client);
	}

	@AfterAll
	void teardown() {
		log.info("Tearing down test instance");
		connection.close();
		client.shutdown();
		client.getResources().shutdown();
		getRedisServer().stop();
	}

	@BeforeEach
	void setupRedis() {
		connection.sync().flushall();
	}

	protected abstract RedisServer getRedisServer();

	protected static Map<String, String> mapOf(String... keyValues) {
		Map<String, String> map = new HashMap<>();
		for (int index = 0; index < keyValues.length / 2; index++) {
			map.put(keyValues[index * 2], keyValues[index * 2 + 1]);
		}
		return map;
	}

	private void createBeerSuggestions() throws IOException {
		MappingIterator<Map<String, Object>> beers = Beers.mapIterator();
		try {
			connection.setAutoFlushCommands(false);
			List<RedisFuture<?>> futures = new ArrayList<>();
			RedisModulesAsyncCommands<String, String> async = connection.async();
			while (beers.hasNext()) {
				Map<String, Object> beer = beers.next();
				futures.add(async.ftSugadd(SUGINDEX,
						Suggestion.string((String) beer.get(Beers.FIELD_NAME.getName())).score(1).build()));
			}
			connection.flushCommands();
			LettuceFutures.awaitAll(RedisURI.DEFAULT_TIMEOUT_DURATION, futures.toArray(new RedisFuture[0]));
		} finally {
			connection.setAutoFlushCommands(true);
		}
	}

	@Test
	void client() {
		RedisServer server = getRedisServer();
		if (server.isCluster()) {
			ping(RedisModulesClusterClient.create(server.getRedisURI()).connect());
			ping(RedisModulesClusterClient
					.create(DefaultClientResources.create(), RedisURI.create(server.getRedisURI())).connect());
			ping(RedisModulesClusterClient.create(server.getRedisURI()).connect(StringCodec.UTF8));
		} else {
			ping(RedisModulesClient.create().connect(RedisURI.create(server.getRedisURI())));
			ping(RedisModulesClient.create(DefaultClientResources.create())
					.connect(RedisURI.create(server.getRedisURI())));
			ping(RedisModulesClient.create(DefaultClientResources.create(), server.getRedisURI()).connect());
			ping(RedisModulesClient.create(DefaultClientResources.create(), RedisURI.create(server.getRedisURI()))
					.connect());
			ping(RedisModulesClient.create().connect(StringCodec.UTF8, RedisURI.create(server.getRedisURI())));
		}
	}

	private void ping(StatefulRedisModulesConnection<String, String> connection) {
		assertEquals("PONG", connection.reactive().ping().block());
	}

	@Nested
	class Search {

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
		@SuppressWarnings("unchecked")
		void create() throws Exception {
			int count = Beers.populateIndex(connection);
			RedisModulesCommands<String, String> sync = connection.sync();
			assertEquals(count, RedisModulesUtils.indexInfo(sync.ftInfo(Beers.INDEX)).getNumDocs());
			CreateOptions<String, String> options = CreateOptions.<String, String>builder().prefix("release:")
					.payloadField("xml").build();
			List<Field<String>> fields = Arrays.asList(Field.text("artist").sortable().build(),
					Field.tag("id").sortable().build(), Field.text("title").sortable().build());
			sync.ftCreate("releases", options, fields.toArray(Field[]::new));
			assertEquals(fields.size(), RedisModulesUtils.indexInfo(sync.ftInfo("releases")).getFields().size());
		}

		@SuppressWarnings("unchecked")
		@Test
		void createTemporaryIndex() throws Exception {
			Beers.populateIndex(connection);
			RedisModulesCommands<String, String> sync = connection.sync();
			String tempIndex = "temporaryIndex";
			sync.ftCreate(tempIndex, CreateOptions.<String, String>builder().temporary(1L).build(),
					Field.text("field1").build());
			assertEquals(tempIndex, RedisModulesUtils.indexInfo(sync.ftInfo(tempIndex)).getIndexName());
			Awaitility.await().until(() -> !sync.ftList().contains(tempIndex));
		}

		@Test
		void alterIndex() throws Exception {
			Beers.populateIndex(connection);
			RedisModulesCommands<String, String> sync = connection.sync();
			Map<String, Object> indexInfo = toMap(sync.ftInfo(Beers.INDEX));
			assertEquals(Beers.INDEX, indexInfo.get("index_name"));
			sync.ftAlter(Beers.INDEX, Field.tag("newField").build());
			Map<String, String> doc = mapOf("newField", "value1");
			sync.hmset("beer:newDoc", doc);
			SearchResults<String, String> results = sync.ftSearch(Beers.INDEX, "@newField:{value1}");
			assertEquals(1, results.getCount());
			assertEquals(doc.get("newField"), results.get(0).get("newField"));
		}

		@Test
		void dropindexDeleteDocs() throws Exception {
			Beers.populateIndex(connection);
			RedisModulesCommands<String, String> sync = connection.sync();
			sync.ftDropindexDeleteDocs(Beers.INDEX);
			Awaitility.await().until(() -> sync.ftList().isEmpty());
			Assertions.assertThrows(RedisCommandExecutionException.class, () -> sync.ftInfo(Beers.INDEX));
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
		void list() throws ExecutionException, InterruptedException {
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
		void searchOptions() throws IOException {
			Beers.populateIndex(connection);
			RedisModulesCommands<String, String> sync = connection.sync();
			SearchOptions<String, String> options = SearchOptions.<String, String>builder().withPayloads(true)
					.noStopWords(true).limit(10, 100).withScores(true)
					.highlight(Highlight.<String, String>builder().field(Beers.FIELD_NAME.getName())
							.tags("<TAG>", "</TAG>").build())
					.language(Language.ENGLISH).noContent(false).timeout(Duration.ofSeconds(10))
					.param("param1", "value").dialect(2).sortBy(SearchOptions.SortBy.asc(Beers.FIELD_NAME.getName()))
					.verbatim(false).withSortKeys().returnField(Beers.FIELD_NAME.getName())
					.returnField(Beers.FIELD_STYLE_NAME.getName()).build();
			SearchResults<String, String> results = sync.ftSearch(Beers.INDEX, "pale", options);
			assertEquals(74, results.getCount());
			Document<String, String> doc1 = results.get(0);
			assertNotNull(doc1.get(Beers.FIELD_NAME.getName()));
			assertNotNull(doc1.get(Beers.FIELD_STYLE_NAME.getName()));
			assertNull(doc1.get(Beers.FIELD_ABV.getName()));

		}

		private void assertSearch(String query, SearchOptions<String, String> options, long expectedCount,
				String... expectedAbv) {
			SearchResults<String, String> results = connection.sync().ftSearch(Beers.INDEX, query, options);
			assertEquals(expectedCount, results.getCount());
			Document<String, String> doc1 = results.get(0);
			assertNotNull(doc1.get(Beers.FIELD_NAME.getName()));
			assertNotNull(doc1.get(Beers.FIELD_STYLE_NAME.getName()));
			if (expectedAbv.length > 0) {
				assertTrue(Arrays.asList(expectedAbv).contains(doc1.get(Beers.FIELD_ABV.getName())));
			}
		}

		@Test
		void search() throws Exception {
			Beers.populateIndex(connection);
			RedisModulesCommands<String, String> sync = connection.sync();
			SearchResults<String, String> results = sync.ftSearch(Beers.INDEX, "German");
			assertEquals(3, results.getCount());
			results = sync.ftSearch(Beers.INDEX, "Hefeweizen",
					SearchOptions.<String, String>builder().noContent(true).build());
			assertEquals(10, results.size());
			assertTrue(results.get(0).getId().startsWith("beer:"));
			results = sync.ftSearch(Beers.INDEX, "Hefeweizen",
					SearchOptions.<String, String>builder().withScores(true).build());
			assertEquals(10, results.size());
			assertTrue(results.get(0).getScore() > 0);
			results = sync.ftSearch(Beers.INDEX, "Hefeweizen", SearchOptions.<String, String>builder().withScores(true)
					.noContent(true).limit(new Limit(0, 100)).build());
			assertEquals(14, results.getCount());
			assertEquals(14, results.size());
			assertTrue(results.get(0).getId().startsWith(Beers.PREFIX));
			assertTrue(results.get(0).getScore() > 0);

			results = sync.ftSearch(Beers.INDEX, "pale",
					SearchOptions.<String, String>builder().withPayloads(true).build());
			assertEquals(74, results.getCount());
			Document<String, String> result1 = results.get(0);
			assertNotNull(result1.get(Beers.FIELD_NAME.getName()));
			assertEquals(result1.get(Beers.FIELD_DESCRIPTION.getName()), result1.getPayload());
			assertEquals(sync.hget(result1.getId(), Beers.FIELD_DESCRIPTION.getName()), result1.getPayload());

			assertSearch("pale", SearchOptions.<String, String>builder().returnField(Beers.FIELD_NAME.getName())
					.returnField(Beers.FIELD_STYLE_NAME.getName()).build(), 74);
			assertSearch("pale", SearchOptions.<String, String>builder().returnField(Beers.FIELD_NAME.getName())
					.returnField(Beers.FIELD_STYLE_NAME.getName()).returnField("").build(), 74);
			assertSearch("*", SearchOptions.<String, String>builder().inKeys("beer:728", "beer:803").build(), 2,
					"5.800000190734863", "8");
			assertSearch("wise", SearchOptions.<String, String>builder().inField(Beers.FIELD_NAME.getName()).build(), 1,
					"5.900000095367432");
		}

		@SuppressWarnings("unchecked")
		@Test
		void searchTags() throws InterruptedException, ExecutionException, IOException {
			int count = Beers.populateIndex(connection);
			RedisModulesCommands<String, String> sync = connection.sync();
			String term = "pale";
			String query = "@style:" + term;
			Tags<String> tags = new Tags<>("<b>", "</b>");
			SearchResults<String, String> results = sync.ftSearch(Beers.INDEX, query,
					SearchOptions.<String, String>builder()
							.highlight(SearchOptions.Highlight.<String, String>builder().build()).build());
			for (Document<String, String> result : results) {
				assertTrue(highlighted(result, Beers.FIELD_STYLE_NAME.getName(), tags, term));
			}
			results = sync.ftSearch(Beers.INDEX, query,
					SearchOptions.<String, String>builder().highlight(
							SearchOptions.Highlight.<String, String>builder().field(Beers.FIELD_NAME.getName()).build())
							.build());
			for (Document<String, String> result : results) {
				assertFalse(highlighted(result, Beers.FIELD_STYLE_NAME.getName(), tags, term));
			}
			tags = new Tags<>("[start]", "[end]");
			results = sync.ftSearch(Beers.INDEX, query,
					SearchOptions.<String, String>builder().highlight(SearchOptions.Highlight.<String, String>builder()
							.field(Beers.FIELD_STYLE_NAME.getName()).tags(tags).build()).build());
			for (Document<String, String> result : results) {
				assertTrue(highlighted(result, Beers.FIELD_STYLE_NAME.getName(), tags, term));
			}

			results = connection.reactive().ftSearch(Beers.INDEX, "pale",
					SearchOptions.<String, String>builder().limit(Limit.offset(10).num(20)).build()).block();
			assertEquals(74, results.getCount());
			Document<String, String> result1 = results.get(0);
			assertNotNull(result1.get(Beers.FIELD_NAME.getName()));
			assertNotNull(result1.get(Beers.FIELD_STYLE_NAME.getName()));
			assertNotNull(result1.get(Beers.FIELD_ABV.getName()));

			results = sync.ftSearch(Beers.INDEX, "pail");
			assertEquals(17, results.getCount());

			results = sync.ftSearch(Beers.INDEX, "*",
					SearchOptions.<String, String>builder().limit(new Limit(0, 0)).build());
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
			SearchResults<String, String> filterResults = sync
					.ftSearch(Beers.INDEX, "*",
							SearchOptions
									.<String, String>builder().filter(SearchOptions.NumericFilter
											.<String, String>field(Beers.FIELD_ABV.getName()).min(minABV).max(maxABV))
									.build());
			assertEquals(10, filterResults.size());
			for (Document<String, String> document : filterResults) {
				double abv = Double.parseDouble(document.get(Beers.FIELD_ABV.getName()));
				Assertions.assertTrue(abv >= minABV);
				Assertions.assertTrue(abv <= maxABV);
			}
		}

		private boolean highlighted(Document<String, String> result, String fieldName, Tags<String> tags,
				String string) {
			String fieldValue = result.get(fieldName).toLowerCase();
			return fieldValue.contains(tags.getOpen() + string + tags.getClose());
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
			int count = Beers.populateIndex(connection);
			MappingIterator<Map<String, Object>> beers = Beers.mapIterator();
			Map<String, Map<String, Object>> beerMap = new HashMap<>();
			while (beers.hasNext()) {
				Map<String, Object> beer = beers.next();
				beerMap.put((String) beer.get(Beers.FIELD_ID.getName()), beer);
			}
			Assertions.assertEquals(count, beerMap.size());
			return beerMap;
		}

		@Test
		void aggregateLoad() throws Exception {
			Map<String, Map<String, Object>> beers = populateBeers();
			Consumer<AggregateResults<String>> loadAsserts = results -> {
				assertEquals(1, results.getCount());
				assertEquals(beers.size(), results.size());
				for (Map<String, Object> result : results) {
					String id = (String) result.get(Beers.FIELD_ID.getName());
					Map<String, Object> beer = beers.get(id);
					assertEquals(((String) beer.get(Beers.FIELD_NAME.getName())).toLowerCase(),
							((String) result.get(Beers.FIELD_NAME.getName())).toLowerCase());
					String style = (String) beer.get("style");
					if (style != null) {
						assertEquals(style.toLowerCase(), ((String) result.get("style")).toLowerCase());
					}
				}

			};
			RedisModulesCommands<String, String> sync = connection.sync();
			RedisModulesReactiveCommands<String, String> reactive = connection.reactive();
			AggregateOptions<String, String> loadOptions = AggregateOptions.<String, String>builder()
					.load(Beers.FIELD_ID.getName()).load(Load.identifier(Beers.FIELD_NAME.getName()).build())
					.load(Load.identifier(Beers.FIELD_STYLE_NAME.getName()).as("style").build()).build();
			loadAsserts.accept(sync.ftAggregate(Beers.INDEX, "*", loadOptions));
			loadAsserts.accept(reactive.ftAggregate(Beers.INDEX, "*", loadOptions).block());
		}

		@Test
		void aggregateGroupSortLimit() throws Exception {
			populateBeers();
			Consumer<AggregateResults<String>> asserts = results -> {
				assertEquals(36, results.getCount());
				List<Double> abvs = results.stream()
						.map(r -> Double.parseDouble((String) r.get(Beers.FIELD_ABV.getName())))
						.collect(Collectors.toList());
				assertTrue(abvs.get(0) > abvs.get(abvs.size() - 1));
				assertEquals(20, results.size());
			};
			AggregateOptions<String, String> options = AggregateOptions
					.<String, String>operation(Group.by(Beers.FIELD_STYLE_NAME.getName())
							.reducer(Avg.property(Beers.FIELD_ABV.getName()).as(Beers.FIELD_ABV.getName()).build())
							.build())
					.operation(Sort.by(Sort.Property.desc(Beers.FIELD_ABV.getName())).build())
					.operation(Limit.offset(0).num(20)).build();
			asserts.accept(connection.sync().ftAggregate(Beers.INDEX, "*", options));
			asserts.accept(connection.reactive().ftAggregate(Beers.INDEX, "*", options).block());
		}

		@Test
		void aggregateSort() throws Exception {
			populateBeers();
			Consumer<AggregateResults<String>> asserts = results -> {
				assertEquals(467, results.getCount());
				List<Double> abvs = results.stream()
						.map(r -> Double.parseDouble((String) r.get(Beers.FIELD_ABV.getName())))
						.collect(Collectors.toList());
				assertTrue(abvs.get(0) > abvs.get(abvs.size() - 1));
				assertEquals(10, results.size());
			};
			AggregateOptions<String, String> options = AggregateOptions
					.<String, String>operation(Sort.by(Sort.Property.desc(Beers.FIELD_ABV.getName()))
							.by(Property.asc(Beers.FIELD_IBU.getName())).build())
					.build();
			asserts.accept(connection.sync().ftAggregate(Beers.INDEX, "*", options));
			asserts.accept(connection.reactive().ftAggregate(Beers.INDEX, "*", options).block());
		}

		@Test
		void aggregateGroupNone() throws Exception {
			populateBeers();
			Consumer<AggregateResults<String>> asserts = results -> {
				assertEquals(1, results.getCount());
				assertEquals(1, results.size());
				Double maxAbv = Double.parseDouble((String) results.get(0).get(Beers.FIELD_ABV.getName()));
				assertEquals(16, maxAbv, 0.1);
			};

			AggregateOptions<String, String> options = AggregateOptions.<String, String>operation(Group.by()
					.reducer(Max.property(Beers.FIELD_ABV.getName()).as(Beers.FIELD_ABV.getName()).build()).build())
					.build();
			asserts.accept(connection.sync().ftAggregate(Beers.INDEX, "*", options));
			asserts.accept(connection.reactive().ftAggregate(Beers.INDEX, "*", options).block());

		}

		@SuppressWarnings("unchecked")
		@Test
		void aggregateGroupToList() throws Exception {
			populateBeers();
			Consumer<AggregateResults<String>> asserts = results -> {
				assertEquals(36, results.getCount());
				Map<String, Object> doc = results.get(1);
				Assumptions.assumeTrue(doc != null);
				Assumptions.assumeTrue(doc.get(Beers.FIELD_STYLE_NAME.getName()) != null);
				String style = ((String) doc.get(Beers.FIELD_STYLE_NAME.getName())).toLowerCase();
				assertTrue(
						style.equals("bamberg-style bock rauchbier") || style.equals("south german-style hefeweizen"));
				int nameCount = ((List<String>) results.get(1).get("names")).size();
				assertEquals(21, nameCount);
			};
			Group group = Group.by(Beers.FIELD_STYLE_NAME.getName())
					.reducer(ToList.property(Beers.FIELD_NAME.getName()).as("names").build()).reducer(Count.as("count"))
					.build();
			AggregateOptions<String, String> options = AggregateOptions.<String, String>operation(group)
					.operation(Limit.offset(0).num(2)).build();
			asserts.accept(connection.sync().ftAggregate(Beers.INDEX, "*", options));
			asserts.accept(connection.reactive().ftAggregate(Beers.INDEX, "*", options).block());
		}

		@Test
		void aggregateCursor() throws Exception {
			populateBeers();
			Consumer<AggregateWithCursorResults<String>> cursorTests = cursorResults -> {
				assertEquals(1, cursorResults.getCount());
				assertEquals(10, cursorResults.size());
				assertTrue(((String) cursorResults.get(9).get(Beers.FIELD_ABV.getName())).length() > 0);
			};
			AggregateOptions<String, String> cursorOptions = AggregateOptions.<String, String>builder()
					.load(Beers.FIELD_ID.getName()).load(Beers.FIELD_NAME.getName()).load(Beers.FIELD_ABV.getName())
					.build();
			AggregateWithCursorResults<String> cursorResults = connection.sync().ftAggregate(Beers.INDEX, "*",
					CursorOptions.builder().count(10).build(), cursorOptions);
			cursorTests.accept(cursorResults);
			cursorTests.accept(connection.reactive()
					.ftAggregate(Beers.INDEX, "*", CursorOptions.builder().count(10).build(), cursorOptions).block());
			cursorResults = connection.sync().ftCursorRead(Beers.INDEX, cursorResults.getCursor(), 400);
			assertEquals(400, cursorResults.size());
			String deleteStatus = connection.sync().ftCursorDelete(Beers.INDEX, cursorResults.getCursor());
			assertEquals("OK", deleteStatus);
		}

		@Test
		void alias() throws Exception {
			Beers.populateIndex(connection);

			// SYNC

			String alias = "alias123";

			RedisModulesCommands<String, String> sync = connection.sync();
			sync.ftAliasadd(alias, Beers.INDEX);
			SearchResults<String, String> results = sync.ftSearch(alias, "*");
			assertTrue(results.size() > 0);

			String newAlias = "alias456";
			sync.ftAliasupdate(newAlias, Beers.INDEX);
			assertTrue(sync.ftSearch(newAlias, "*").size() > 0);

			sync.ftAliasdel(newAlias);
			Assertions.assertThrows(RedisCommandExecutionException.class, () -> sync.ftSearch(newAlias, "*"),
					"no such index");

			sync.ftAliasdel(alias);
			RedisModulesAsyncCommands<String, String> async = connection.async();
			// ASYNC
			async.ftAliasadd(alias, Beers.INDEX).get();
			results = async.ftSearch(alias, "*").get();
			assertTrue(results.size() > 0);

			async.ftAliasupdate(newAlias, Beers.INDEX).get();
			assertTrue(async.ftSearch(newAlias, "*").get().size() > 0);

			async.ftAliasdel(newAlias).get();
			Assertions.assertThrows(ExecutionException.class, () -> async.ftSearch(newAlias, "*").get(),
					"no such index");

			sync.ftAliasdel(alias);

			RedisModulesReactiveCommands<String, String> reactive = connection.reactive();
			// REACTIVE
			reactive.ftAliasadd(alias, Beers.INDEX).block();
			results = reactive.ftSearch(alias, "*").block();
			assertTrue(results.size() > 0);

			reactive.ftAliasupdate(newAlias, Beers.INDEX).block();
			results = reactive.ftSearch(newAlias, "*").block();
			Assertions.assertFalse(results.isEmpty());

			reactive.ftAliasdel(newAlias).block();
			Mono<SearchResults<String, String>> searchResults = reactive.ftSearch(newAlias, "*");
			Assertions.assertThrows(RedisCommandExecutionException.class, () -> searchResults.block(), "no such index");
		}

		@Test
		void info() throws Exception {
			int count = Beers.populateIndex(connection);
			IndexInfo info = RedisModulesUtils.indexInfo(connection.async().ftInfo(Beers.INDEX).get());
			assertEquals(count, info.getNumDocs());
			List<Field<String>> fields = info.getFields();
			TextField<String> descriptionField = (TextField<String>) fields.get(5);
			assertEquals(Beers.FIELD_DESCRIPTION.getName(), descriptionField.getName());
			Assertions.assertFalse(descriptionField.isNoIndex());
			Assertions.assertTrue(descriptionField.isNoStem());
			Assertions.assertFalse(descriptionField.isSortable());
			TagField<String> styleField = (TagField<String>) fields.get(2);
			assertEquals(Beers.FIELD_STYLE_NAME.getName(), styleField.getName());
			Assertions.assertTrue(styleField.isSortable());
			assertEquals(',', styleField.getSeparator().get());
		}

		@SuppressWarnings("unchecked")
		@Test
		void jsonSearch() throws Exception {
			Iterator<JsonNode> iterator = Beers.jsonNodeIterator();
			String index = "beers";
			TagField<String> idField = Field.tag(jsonField(Beers.FIELD_ID.getName())).as(Beers.FIELD_ID.getName())
					.build();
			TextField<String> nameField = Field.text(jsonField(Beers.FIELD_NAME.getName()))
					.as(Beers.FIELD_NAME.getName()).build();
			TextField<String> styleField = Field.text(jsonField(Beers.FIELD_STYLE_NAME.getName())).build();
			connection.sync().ftCreate(index, CreateOptions.<String, String>builder().on(DataType.JSON).build(),
					idField, nameField, styleField);
			IndexInfo info = RedisModulesUtils.indexInfo(connection.sync().ftInfo(index));
			Assertions.assertEquals(3, info.getFields().size());
			Assertions.assertEquals(idField.getAs(), info.getFields().get(0).getAs());
			Assertions.assertEquals(styleField.getName(), info.getFields().get(2).getAs().get());
			while (iterator.hasNext()) {
				JsonNode beer = iterator.next();
				connection.sync().jsonSet("beer:" + beer.get(Beers.FIELD_ID.getName()).asText(), "$", beer.toString());
			}
			SearchResults<String, String> results = connection.sync().ftSearch(index,
					"@" + Beers.FIELD_NAME.getName() + ":Creek");
			Assertions.assertEquals(1, results.getCount());
		}

		private String jsonField(String name) {
			return "$." + name;
		}

		@SuppressWarnings("unchecked")
		@Test
		void infoFields() {
			String index = "indexFields";
			TagField<String> idField = Field.tag(jsonField(Beers.FIELD_ID.getName())).as(Beers.FIELD_ID.getName())
					.separator('-').build();
			TextField<String> nameField = Field.text(jsonField(Beers.FIELD_NAME.getName()))
					.as(Beers.FIELD_NAME.getName()).noIndex().noStem().unNormalizedForm().weight(2).build();
			String styleFieldName = jsonField(Beers.FIELD_STYLE_NAME.getName());
			TextField<String> styleField = Field.text(styleFieldName).as(styleFieldName).weight(1).build();
			connection.sync().ftCreate(index, CreateOptions.<String, String>builder().on(DataType.JSON).build(),
					idField, nameField, styleField);
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
		void infoOptions() {
			RedisModulesCommands<String, String> commands = connection.sync();
			String index = "indexWithOptions";
			CreateOptions<String, String> createOptions = CreateOptions.<String, String>builder().on(DataType.JSON)
					.prefixes("prefix1", "prefix2").filter("@indexName==\"myindexname\"")
					.defaultLanguage(Language.CHINESE).languageField("languageField").defaultScore(.5)
					.scoreField("scoreField").payloadField("payloadField").maxTextFields(true).noOffsets(true)
					.noFields(true).noFreqs(true).build();
			commands.ftCreate(index, createOptions, Field.tag("id").build(), Field.numeric("scoreField").build());
			IndexInfo info = RedisModulesUtils.indexInfo(commands.ftInfo(index));
			Assertions.assertEquals(createOptions, info.getIndexOptions());
		}

		@Test
		void tagVals() throws Exception {
			Beers.populateIndex(connection);
			Set<String> TAG_VALS = new HashSet<>(Arrays.asList(
					"american-style brown ale, traditional german-style bock, german-style schwarzbier, old ale, american-style india pale ale, german-style oktoberfest, other belgian-style ales, american-style stout, winter warmer, belgian-style tripel, american-style lager, belgian-style dubbel, porter, american-style barley wine ale, belgian-style fruit lambic, scottish-style light ale, south german-style hefeweizen, imperial or double india pale ale, golden or blonde ale, belgian-style quadrupel, american-style imperial stout, belgian-style pale strong ale, english-style pale mild ale, american-style pale ale, irish-style red ale, dark american-belgo-style ale, light american wheat ale or lager, german-style pilsener, american-style amber/red ale, scotch ale, german-style doppelbock, extra special bitter, south german-style weizenbock, english-style india pale ale, belgian-style pale ale, french & belgian-style saison"
							.split(", ")));
			HashSet<String> actual = new HashSet<>(
					connection.sync().ftTagvals(Beers.INDEX, Beers.FIELD_STYLE_NAME.getName()));
			assertEquals(TAG_VALS, actual);
			assertEquals(TAG_VALS, new HashSet<>(connection.reactive()
					.ftTagvals(Beers.INDEX, Beers.FIELD_STYLE_NAME.getName()).collectList().block()));
		}

		@SuppressWarnings("unchecked")
		@Test
		void emptyToListReducer() {
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
		void dictadd() {
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
	}

	@Nested
	class TimeSeries {

		private static final String LABEL_SENSOR_ID = "sensor_id";
		private static final String LABEL_AREA_ID = "area_id";
		private static final long TIMESTAMP_1 = 1548149181;
		private static final long TIMESTAMP_2 = 1548149191;
		private static final double VALUE_1 = 30;
		private static final double VALUE_2 = 42;
		private static final String KEY = "temperature:3:11";
		private static final String KEY_2 = "temperature:3:12";
		private static final String SENSOR_ID = "2";
		private static final String AREA_ID = "32";
		private static final String AREA_ID_2 = "34";
		private static final String FILTER = LABEL_SENSOR_ID + "=" + SENSOR_ID;

		@SuppressWarnings("unchecked")
		@Test
		void create() {
			String status = connection.sync().tsCreate(KEY, com.redis.lettucemod.timeseries.CreateOptions
					.<String, String>builder().retentionPeriod(6000).build());
			assertEquals("OK", status);
			assertEquals("OK",
					connection.sync().tsCreate("virag",
							com.redis.lettucemod.timeseries.CreateOptions.<String, String>builder()
									.retentionPeriod(100000L).labels(Label.of("name", "value"))
									.policy(DuplicatePolicy.LAST).build()));
		}

		@SuppressWarnings("unchecked")
		@Test
		void add() {
			RedisTimeSeriesCommands<String, String> ts = connection.sync();
			// TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
			// TS.ADD temperature:3:11 1548149181 30
			Long add1 = ts.tsAdd(KEY, Sample.of(TIMESTAMP_1, VALUE_1),
					AddOptions.<String, String>builder().retentionPeriod(6000)
							.labels(Label.of(LABEL_SENSOR_ID, SENSOR_ID), Label.of(LABEL_AREA_ID, AREA_ID)).build());
			assertEquals(TIMESTAMP_1, add1);
			List<GetResult<String, String>> results = ts.tsMget(FILTER);
			assertEquals(1, results.size());
			assertEquals(TIMESTAMP_1, results.get(0).getSample().getTimestamp());
			// TS.ADD temperature:3:11 1548149191 42
			Long add2 = ts.tsAdd(KEY, Sample.of(TIMESTAMP_2, VALUE_2));
			assertEquals(TIMESTAMP_2, add2);
		}

		@Test
		void range() {
			RedisTimeSeriesCommands<String, String> ts = connection.sync();
			populate(ts);
			assertRange(ts.tsRange(KEY, TimeRange.builder().from(TIMESTAMP_1 - 10).to(TIMESTAMP_2 + 10).build(),
					RangeOptions.builder()
							.aggregation(
									Aggregation.aggregator(Aggregator.AVG).bucketDuration(Duration.ofMillis(5)).build())
							.build()));
			assertRange(ts.tsRange(KEY, TimeRange.unbounded(),
					RangeOptions.builder()
							.aggregation(
									Aggregation.aggregator(Aggregator.AVG).bucketDuration(Duration.ofMillis(5)).build())
							.build()));
			assertRange(ts.tsRange(KEY, TimeRange.from(TIMESTAMP_1 - 10).build(),
					RangeOptions.builder()
							.aggregation(
									Aggregation.aggregator(Aggregator.AVG).bucketDuration(Duration.ofMillis(5)).build())
							.build()));
			assertRange(ts.tsRange(KEY, TimeRange.to(TIMESTAMP_2 + 10).build(),
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
		private void populate(RedisTimeSeriesCommands<String, String> ts) {
			// TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
			// TS.ADD temperature:3:11 1548149181 30
			ts.tsAdd(KEY, Sample.of(TIMESTAMP_1, VALUE_1), AddOptions.<String, String>builder().retentionPeriod(6000)
					.labels(Label.of(LABEL_SENSOR_ID, SENSOR_ID), Label.of(LABEL_AREA_ID, AREA_ID)).build());
			// TS.ADD temperature:3:11 1548149191 42
			ts.tsAdd(KEY, Sample.of(TIMESTAMP_2, VALUE_2));

			ts.tsAdd(KEY_2, Sample.of(TIMESTAMP_1, VALUE_1), AddOptions.<String, String>builder().retentionPeriod(6000)
					.labels(Label.of(LABEL_SENSOR_ID, SENSOR_ID), Label.of(LABEL_AREA_ID, AREA_ID_2)).build());
			ts.tsAdd(KEY_2, Sample.of(TIMESTAMP_2, VALUE_2));
		}

		@Test
		void mget() {
			RedisTimeSeriesCommands<String, String> ts = connection.sync();
			populate(ts);
			List<GetResult<String, String>> results = ts.tsMget(FILTER);
			Assertions.assertEquals(2, results.size());
			Assertions.assertEquals(TIMESTAMP_2, results.get(0).getSample().getTimestamp());
			Assertions.assertEquals(VALUE_2, results.get(0).getSample().getValue());
			Assertions.assertEquals(TIMESTAMP_2, results.get(1).getSample().getTimestamp());
			Assertions.assertEquals(VALUE_2, results.get(1).getSample().getValue());
		}

		@Test
		void get() {
			RedisTimeSeriesCommands<String, String> ts = connection.sync();
			populate(ts);
			Sample result = ts.tsGet(KEY);
			Assertions.assertEquals(TIMESTAMP_2, result.getTimestamp());
			Assertions.assertEquals(VALUE_2, result.getValue());
			ts.tsCreate("ts:empty", com.redis.lettucemod.timeseries.CreateOptions.<String, String>builder().build());
			Assertions.assertNull(ts.tsGet("ts:empty"));
		}

		@Test
		void mrange() {
			RedisTimeSeriesCommands<String, String> ts = connection.sync();
			populate(ts);
			List<String> keys = Arrays.asList(KEY, KEY_2);
			assertMrange(keys,
					ts.tsMrange(TimeRange.unbounded(), MRangeOptions.<String, String>filters(FILTER).build()));
			assertMrange(keys, ts.tsMrange(TimeRange.from(TIMESTAMP_1 - 10).build(),
					MRangeOptions.<String, String>filters(FILTER).build()));
			assertMrange(keys, ts.tsMrange(TimeRange.to(TIMESTAMP_2 + 10).build(),
					MRangeOptions.<String, String>filters(FILTER).build()));
			List<RangeResult<String, String>> results = ts.tsMrange(TimeRange.unbounded(),
					MRangeOptions.<String, String>filters(FILTER).withLabels().build());
			assertEquals(2, results.size());
			RangeResult<String, String> key1Result;
			RangeResult<String, String> key2Result;
			if (results.get(0).getKey().equals(KEY)) {
				key1Result = results.get(0);
				key2Result = results.get(1);
			} else {
				key1Result = results.get(1);
				key2Result = results.get(0);
			}
			assertEquals(KEY, key1Result.getKey());
			assertEquals(2, key1Result.getSamples().size());
			assertEquals(TIMESTAMP_1, key1Result.getSamples().get(0).getTimestamp());
			assertEquals(VALUE_1, key1Result.getSamples().get(0).getValue());
			assertEquals(TIMESTAMP_2, key1Result.getSamples().get(1).getTimestamp());
			assertEquals(VALUE_2, key1Result.getSamples().get(1).getValue());
			assertEquals(2, key1Result.getLabels().size());
			assertEquals(SENSOR_ID, key1Result.getLabels().get(LABEL_SENSOR_ID));
			assertEquals(AREA_ID, key1Result.getLabels().get(LABEL_AREA_ID));
			assertEquals(KEY_2, key2Result.getKey());
			assertEquals(2, key2Result.getSamples().size());
			assertEquals(TIMESTAMP_1, key2Result.getSamples().get(0).getTimestamp());
			assertEquals(VALUE_1, key2Result.getSamples().get(0).getValue());
			assertEquals(TIMESTAMP_2, key2Result.getSamples().get(1).getTimestamp());
			assertEquals(VALUE_2, key2Result.getSamples().get(1).getValue());
			assertEquals(2, key2Result.getLabels().size());
			assertEquals(SENSOR_ID, key2Result.getLabels().get(LABEL_SENSOR_ID));
			assertEquals(AREA_ID_2, key2Result.getLabels().get(LABEL_AREA_ID));
		}

		private void assertMrange(List<String> keys, List<RangeResult<String, String>> results) {
			assertEquals(2, results.size());
			assertEquals(new HashSet<>(keys), results.stream().map(RangeResult::getKey).collect(Collectors.toSet()));
			assertEquals(2, results.get(0).getSamples().size());
			assertEquals(TIMESTAMP_1, results.get(0).getSamples().get(0).getTimestamp());
			assertEquals(VALUE_1, results.get(0).getSamples().get(0).getValue());
			assertEquals(TIMESTAMP_2, results.get(0).getSamples().get(1).getTimestamp());
			assertEquals(VALUE_2, results.get(0).getSamples().get(1).getValue());
		}
	}

	@Nested
	class Json {

		private static final String JSON = "{\"name\":\"Leonard Cohen\",\"lastSeen\":1478476800,\"loggedOut\": true}";

		@Test
		void set() {
			RedisJSONCommands<String, String> sync = connection.sync();
			String result = sync.jsonSet("obj", ".", JSON);
			assertEquals("OK", result);
		}

		@Test
		void setNX() {
			RedisJSONCommands<String, String> sync = connection.sync();
			sync.jsonSet("obj", ".", JSON);
			String result = sync.jsonSet("obj", ".", JSON, SetMode.NX);
			Assertions.assertNull(result);
		}

		@Test
		void setXX() {
			RedisJSONCommands<String, String> sync = connection.sync();
			String result = sync.jsonSet("obj", ".", "true", SetMode.XX);
			Assertions.assertNull(result);
		}

		@Test
		void get() throws JsonProcessingException {
			RedisJSONCommands<String, String> sync = connection.sync();
			sync.jsonSet("obj", ".", JSON);
			String result = sync.jsonGet("obj");
			assertJSONEquals(JSON, result);
		}

		@Test
		void getPaths() throws JsonProcessingException {
			RedisJSONCommands<String, String> sync = connection.sync();
			sync.jsonSet("obj", ".", JSON);
			String result = sync.jsonGet("obj", ".name", ".loggedOut");
			assertJSONEquals("{\".name\":\"Leonard Cohen\",\".loggedOut\": true}", result);
		}

		@Test
		void getOptions() {
			RedisJSONCommands<String, String> sync = connection.sync();
			sync.jsonSet("obj", ".", JSON);
			String result = sync.jsonGet("obj",
					GetOptions.builder().indent("___").newline("#").noEscape(true).space("_").build());
			assertEquals("{#___\"name\":_\"Leonard Cohen\",#___\"lastSeen\":_1478476800,#___\"loggedOut\":_true#}",
					result);
		}

		@Test
		void getOptionsPaths() throws JsonMappingException, JsonProcessingException {
			RedisJSONCommands<String, String> sync = connection.sync();
			sync.jsonSet("obj", ".", JSON);
			String result = sync.jsonGet("obj",
					GetOptions.builder().indent("  ").newline("\n").noEscape(true).space("   ").build(), ".name",
					".loggedOut");
			JsonNode resultNode = new ObjectMapper().readTree(result);
			assertEquals("Leonard Cohen", resultNode.get(".name").asText());
		}

		@Test
		void mget() throws JsonProcessingException {
			RedisJSONCommands<String, String> sync = connection.sync();
			sync.jsonSet("obj1", ".", JSON);
			String json2 = "{\"name\":\"Herbie Hancock\",\"lastSeen\":1478476810,\"loggedOut\": false}";
			sync.jsonSet("obj2", ".", json2);
			String json3 = "{\"name\":\"Lalo Schifrin\",\"lastSeen\":1478476820,\"loggedOut\": false}";
			sync.jsonSet("obj3", ".", json3);

			List<KeyValue<String, String>> results = sync.jsonMget(".", "obj1", "obj2", "obj3");
			assertEquals(3, results.size());
			assertEquals("obj1", results.get(0).getKey());
			assertEquals("obj2", results.get(1).getKey());
			assertEquals("obj3", results.get(2).getKey());
			assertJSONEquals(JSON, results.get(0).getValue());
			assertJSONEquals(json2, results.get(1).getValue());
			assertJSONEquals(json3, results.get(2).getValue());
		}

		@Test
		void del() {
			RedisJSONCommands<String, String> sync = connection.sync();
			sync.jsonSet("obj", ".", JSON);
			sync.jsonDel("obj");
			String result = sync.jsonGet("obj");
			Assertions.assertNull(result);
		}

		@Test
		void type() {
			RedisJSONCommands<String, String> sync = connection.sync();
			sync.jsonSet("obj", ".", JSON);
			assertEquals("object", sync.jsonType("obj"));
			assertEquals("string", sync.jsonType("obj", ".name"));
			assertEquals("boolean", sync.jsonType("obj", ".loggedOut"));
			assertEquals("integer", sync.jsonType("obj", ".lastSeen"));
		}

		@Test
		void numIncrBy() {
			RedisJSONCommands<String, String> sync = connection.sync();
			sync.jsonSet("obj", ".", JSON);
			long lastSeen = 1478476800;
			double increment = 123.456;
			String result = sync.jsonNumincrby("obj", ".lastSeen", increment);
			assertEquals(lastSeen + increment, Double.parseDouble(result));
		}

		@Test
		void numMultBy() {
			RedisJSONCommands<String, String> sync = connection.sync();
			sync.jsonSet("obj", ".", JSON);
			long lastSeen = 1478476800;
			double factor = 123.456;
			String result = sync.jsonNummultby("obj", ".lastSeen", factor);
			assertEquals(lastSeen * factor, Double.parseDouble(result));
		}

		@Test
		void strings() {
			RedisJSONCommands<String, String> sync = connection.sync();
			sync.jsonSet("foo", ".", "\"bar\"");
			assertEquals(3, sync.jsonStrlen("foo", "."));
			assertEquals("barbaz".length(), sync.jsonStrappend("foo", ".", "\"baz\""));
		}

		@Test
		void arrays() {
			RedisJSONCommands<String, String> sync = connection.sync();
			String key = "arr";
			sync.jsonSet(key, ".", "[]");
			assertEquals(1, sync.jsonArrappend(key, ".", "0"));
			assertEquals("[0]", sync.jsonGet(key));
			assertEquals(3, sync.jsonArrinsert(key, ".", 0, "-2", "-1"));
			assertEquals("[-2,-1,0]", sync.jsonGet(key));
			assertEquals(1, sync.jsonArrindex(key, ".", "-1"));
			assertEquals(1, sync.jsonArrindex(key, ".", "-1", Slice.start(0).stop(3)));
			assertEquals(1, sync.jsonArrtrim(key, ".", 1, 1));
			assertEquals("[-1]", sync.jsonGet(key));
			assertEquals("-1", sync.jsonArrpop(key));
		}

		@Test
		void objects() {
			RedisJSONCommands<String, String> sync = connection.sync();
			sync.jsonSet("obj", ".", JSON);
			assertEquals(3, sync.jsonObjlen("obj", "."));
			assertEquals(Arrays.asList("name", "lastSeen", "loggedOut"), sync.jsonObjkeys("obj", "."));
		}
	}

	@EnabledOnOs(OS.LINUX)
	@Nested
	class Gears {

		@Test
		void pyExecute() {
			RedisModulesCommands<String, String> sync = connection.sync();
			sync.set("foo", "bar");
			ExecutionResults results = pyExecute(sync, "sleep.py");
			assertEquals("1", results.getResults().get(0));
		}

		@Test
		void pyExecuteUnblocking() {
			RedisModulesCommands<String, String> sync = connection.sync();
			sync.set("foo", "bar");
			String executionId = pyExecuteUnblocking(sync, "sleep.py");
			String[] array = executionId.split("-");
			assertEquals(2, array.length);
			assertEquals(40, array[0].length());
			Assertions.assertTrue(Integer.parseInt(array[1]) >= 0);
		}

//    
//    
//    void pyExecuteNoResults() {
//        RedisModulesCommands<String, String> sync = context.sync();
//        ExecutionResults results = pyExecute(sync, "sleep.py");
//        Assertions.assertTrue(results.getResults().isEmpty());
//        Assertions.assertTrue(results.getErrors().isEmpty());
//    }

		private ExecutionResults pyExecute(RedisGearsCommands<String, String> sync, String resourceName) {
			return sync.rgPyexecute(load(resourceName));
		}

		private String pyExecuteUnblocking(RedisGearsCommands<String, String> sync, String resourceName) {
			return sync.rgPyexecuteUnblocking(load(resourceName));
		}

		private String load(String resourceName) {
			return RedisModulesUtils.toString(getClass().getClassLoader().getResourceAsStream(resourceName));
		}

		private void clearGears() throws InterruptedException {
			RedisModulesCommands<String, String> sync = connection.sync();
			// Unregister all registrations
			for (Registration registration : sync.rgDumpregistrations()) {
				log.info("Unregistering {}", registration.getId());
				sync.rgUnregister(registration.getId());
			}
			// Drop all executions
			for (Execution execution : sync.rgDumpexecutions()) {
				if (execution.getStatus().matches("running|created")) {
					log.info("Aborting execution {} with status {}", execution.getId(), execution.getStatus());
					sync.rgAbortexecution(execution.getId());
				}
				try {
					sync.rgDropexecution(execution.getId());
				} catch (RedisCommandExecutionException e) {
					log.info("Execution status: {}", execution.getStatus());
					throw e;
				}
			}
		}

		@Test
		void dumpRegistrations() throws InterruptedException {
			clearGears();
			RedisModulesCommands<String, String> sync = connection.sync();
			// Single registration
			List<Registration> registrations = sync.rgDumpregistrations();
			assertEquals(0, registrations.size());
			ExecutionResults results = pyExecute(sync, "streamreader.py");
			Assertions.assertFalse(results.isError());
			registrations = sync.rgDumpregistrations();
			assertEquals(1, registrations.size());
			Registration registration = registrations.get(0);
			assertEquals("StreamReader", registration.getReader());
			assertEquals("MyStreamReader", registration.getDescription());
			assertEquals("async", registration.getData().getMode());
			Map<String, Object> args = registration.getData().getArgs();
			assertTrue(args.size() >= 3);
			assertEquals(1L, args.get("batchSize"));
			assertEquals("mystream", args.get("stream"));
			assertEquals("OK", registration.getData().getStatus());

			// Multiple registrations
			sync.rgDumpregistrations().forEach(r -> sync.rgUnregister(r.getId()));
			String function = "GB('KeysReader').register('*', keyTypes=['hash'])";
			Assertions.assertTrue(sync.rgPyexecute(function).isOk());
			Assertions.assertEquals(1, sync.rgDumpregistrations().size());
		}

		@Test
		void pyExecuteResults() {
			RedisModulesCommands<String, String> sync = connection.sync();
			sync.set("foo", "bar");
			ExecutionResults results = sync.rgPyexecute("GB().foreach(lambda x: log('test')).register()");
			Assertions.assertTrue(results.isOk());
			Assertions.assertFalse(results.isError());
		}

		private void executions() {
			RedisModulesCommands<String, String> sync = connection.sync();
			sync.set("foo", "bar");
			pyExecuteUnblocking(sync, "sleep.py");
		}

		@Test
		void dumpExecutions() throws InterruptedException {
			clearGears();
			executions();
			assertFalse(connection.sync().rgDumpexecutions().isEmpty());
		}

		@Test
		void dropExecution() throws InterruptedException {
			clearGears();
			executions();
			RedisModulesCommands<String, String> sync = connection.sync();
			List<Execution> executions = sync.rgDumpexecutions();
			executions.forEach(e -> sync.rgAbortexecution(e.getId()));
			executions.forEach(e -> sync.rgDropexecution(e.getId()));
			assertEquals(0, sync.rgDumpexecutions().size());
		}

		@Test
		void abortExecution() throws InterruptedException {
			clearGears();
			executions();
			RedisModulesCommands<String, String> sync = connection.sync();
			for (Execution execution : sync.rgDumpexecutions()) {
				sync.rgAbortexecution(execution.getId());
				ExecutionDetails details = sync.rgGetexecution(execution.getId());
				Assertions.assertTrue(details.getPlan().getStatus().matches("done|aborted"));
			}
		}

	}

	@Nested
	class Utils {

		@Test
		void credentials() {
			String username = "alice";
			String password = "ecila";
			connection.sync().aclSetuser(username,
					AclSetuserArgs.Builder.on().addPassword(password).allCommands().allKeys());
			try {
				AbstractRedisClient client = ClientBuilder
						.create(RedisURIBuilder.create(getRedisServer().getRedisURI()).username(username)
								.password("wrongpassword".toCharArray()).build())
						.cluster(getRedisServer().isCluster()).build();
				RedisModulesUtils.connection(client);
				Assertions.fail("Expected connection failure");
			} catch (Exception e) {
				// expected
			}
			String key = "foo";
			String value = "bar";
			RedisURI uri = RedisURIBuilder.create(getRedisServer().getRedisURI()).username(username)
					.password(password.toCharArray()).build();
			StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils
					.connection(ClientBuilder.create(uri).cluster(getRedisServer().isCluster()).build());
			connection.sync().set(key, value);
			Assertions.assertEquals(value, connection.sync().get(key));
		}

		@Test
		void hostAndPort() {
			RedisURI redisURI = RedisURI.create(getRedisServer().getRedisURI());
			RedisURI uri = RedisURIBuilder.create().host(redisURI.getHost()).port(redisURI.getPort()).build();
			StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils
					.connection(ClientBuilder.create(uri).cluster(getRedisServer().isCluster()).build());
			Assertions.assertEquals("PONG", connection.sync().ping());
		}
	}

	protected void assertJSONEquals(String expected, String actual)
			throws JsonMappingException, JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		assertEquals(mapper.readTree(expected), mapper.readTree(actual));
	}

}
