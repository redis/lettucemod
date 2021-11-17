package com.redis.lettucemod.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import com.fasterxml.jackson.databind.MappingIterator;
import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.reactive.RedisModulesReactiveCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateResults;
import com.redis.lettucemod.search.AggregateWithCursorResults;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Cursor;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.GroupOperation;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.Language;
import com.redis.lettucemod.search.LimitOperation;
import com.redis.lettucemod.search.Order;
import com.redis.lettucemod.search.Reducers.Avg;
import com.redis.lettucemod.search.Reducers.Count;
import com.redis.lettucemod.search.Reducers.Max;
import com.redis.lettucemod.search.Reducers.ToList;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchOptions.Highlight;
import com.redis.lettucemod.search.SearchOptions.Highlight.Tags;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.SortOperation;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.testcontainers.junit.jupiter.RedisTestContext;
import com.redis.testcontainers.junit.jupiter.RedisTestContextsSource;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;

class SearchTests extends AbstractLettuceModTestBase {

	protected final static String SUGINDEX = "beersSug";

	@Test
	void testCreate() {
		testPing(RedisModulesClusterClient.create(REDIS_ENTERPRISE.getRedisURI()).connect());
		testPing(RedisModulesClusterClient
				.create(DefaultClientResources.create(), RedisURI.create(REDIS_ENTERPRISE.getRedisURI())).connect());
		testPing(RedisModulesClusterClient.create(REDIS_ENTERPRISE.getRedisURI()).connect(StringCodec.UTF8));
	}

	private void testPing(StatefulRedisModulesConnection<String, String> connection) {
		Assertions.assertEquals("PONG", connection.reactive().ping().block());
	}

	protected static Map<String, String> mapOf(String... keyValues) {
		Map<String, String> map = new HashMap<>();
		for (int index = 0; index < keyValues.length / 2; index++) {
			map.put(keyValues[index * 2], keyValues[index * 2 + 1]);
		}
		return map;
	}

	private void createBeerSuggestions(RedisTestContext context) throws IOException {
		MappingIterator<Map<String, Object>> beers = Beers.mapIterator();
		RedisModulesAsyncCommands<String, String> async = context.async();
		async.setAutoFlushCommands(false);
		List<RedisFuture<?>> futures = new ArrayList<>();
		while (beers.hasNext()) {
			Map<String, Object> beer = beers.next();
			futures.add(async.sugadd(SUGINDEX, (String) beer.get(Beers.FIELD_NAME.getName()), 1));
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
		RedisModulesUtils.GeoLocation location = RedisModulesUtils.GeoLocation.of(locationString);
		Assertions.assertEquals(longitude, location.getLongitude());
		Assertions.assertEquals(latitude, location.getLatitude());
		Assertions.assertEquals(locationString,
				RedisModulesUtils.GeoLocation.toString(String.valueOf(longitude), String.valueOf(latitude)));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSugaddIncr(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		String key = "testSugadd";
		sync.sugadd(key, "value1", 1);
		sync.sugaddIncr(key, "value1", 1);
		List<Suggestion<String>> suggestions = sync.sugget(key, "value",
				SuggetOptions.builder().withScores(true).build());
		Assertions.assertEquals(1, suggestions.size());
		Assertions.assertEquals(1.4142135381698608, suggestions.get(0).getScore());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSugaddPayload(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		String key = "testSugadd";
		sync.sugadd(key, "value1", 1, "somepayload");
		List<Suggestion<String>> suggestions = sync.sugget(key, "value",
				SuggetOptions.builder().withPayloads(true).build());
		Assertions.assertEquals(1, suggestions.size());
		Assertions.assertEquals("somepayload", suggestions.get(0).getPayload());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSugaddScorePayload(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		String key = "testSugadd";
		sync.sugadd(key, "value1", 2, "somepayload");
		List<Suggestion<String>> suggestions = sync.sugget(key, "value",
				SuggetOptions.builder().withScores(true).withPayloads(true).build());
		Assertions.assertEquals(1, suggestions.size());
		Assertions.assertEquals(1.4142135381698608, suggestions.get(0).getScore());
		Assertions.assertEquals("somepayload", suggestions.get(0).getPayload());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void create(RedisTestContext context) throws Exception {
		int count = Beers.populateIndex(context.getConnection());
		RedisModulesCommands<String, String> sync = context.sync();
		assertEquals(count, RedisModulesUtils.indexInfo(sync.indexInfo(Beers.INDEX)).getNumDocs());
		CreateOptions<String, String> options = CreateOptions.<String, String>builder().prefix("release:")
				.payloadField("xml").build();
		Field[] fields = new Field[] { Field.text("artist").sortable().build(), Field.tag("id").sortable().build(),
				Field.text("title").sortable().build() };
		sync.create("releases", options, fields);
		Assertions.assertEquals(fields.length,
				RedisModulesUtils.indexInfo(sync.indexInfo("releases")).getFields().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void createTemporaryIndex(RedisTestContext context) throws Exception {
		Beers.populateIndex(context.getConnection());
		RedisModulesCommands<String, String> sync = context.sync();
		String tempIndex = "temporaryIndex";
		sync.create(tempIndex, CreateOptions.<String, String>builder().temporary(1L).build(),
				Field.text("field1").build());
		assertEquals(tempIndex, RedisModulesUtils.indexInfo(sync.indexInfo(tempIndex)).getIndexName());
		Thread.sleep(1500);
		try {
			sync.indexInfo(tempIndex);
			fail("Temporary index not deleted");
		} catch (RedisCommandExecutionException e) {
			assertEquals("Unknown Index name", e.getMessage());
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void alterIndex(RedisTestContext context) throws Exception {
		Beers.populateIndex(context.getConnection());
		RedisModulesCommands<String, String> sync = context.sync();
		Map<String, Object> indexInfo = toMap(sync.indexInfo(Beers.INDEX));
		assertEquals(Beers.INDEX, indexInfo.get("index_name"));
		sync.alter(Beers.INDEX, Field.tag("newField").build());
		Map<String, String> doc = mapOf("newField", "value1");
		sync.hmset("beer:newDoc", doc);
		SearchResults<String, String> results = sync.search(Beers.INDEX, "@newField:{value1}");
		assertEquals(1, results.getCount());
		assertEquals(doc.get("newField"), results.get(0).get("newField"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testDropIndexDeleteDocs(RedisTestContext context) throws Exception {
		Beers.populateIndex(context.getConnection());
		RedisModulesCommands<String, String> sync = context.sync();
		sync.dropindexDeleteDocs(Beers.INDEX);
		Awaitility.await().until(() -> sync.list().isEmpty());
		try {
			sync.indexInfo(Beers.INDEX);
			Assertions.fail("Expected unknown index exception");
		} catch (RedisCommandExecutionException e) {
			// ignore
		}
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
	@RedisTestContextsSource
	void list(RedisTestContext context) throws ExecutionException, InterruptedException {
		RedisModulesCommands<String, String> sync = context.sync();
		sync.flushall();
		Set<String> indexNames = new HashSet<>(Arrays.asList("index1", "index2", "index3"));
		for (String indexName : indexNames) {
			sync.create(indexName, Field.text("field1").sortable().build());
		}
		assertEquals(indexNames, new HashSet<>(sync.list()));
		assertEquals(indexNames, new HashSet<>(context.async().list().get()));
		assertEquals(indexNames, new HashSet<>(context.reactive().list().collectList().block()));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void searchOptions(RedisTestContext context) throws IOException {
		Beers.populateIndex(context.getConnection());
		RedisModulesCommands<String, String> sync = context.sync();
		SearchOptions<String, String> options = SearchOptions.<String, String>builder().withPayloads(true)
				.noStopWords(true).limit(SearchOptions.limit(10, 100)).withScores(true)
				.highlight(Highlight.<String, String>builder().field(Beers.FIELD_NAME.getName()).tags("<TAG>", "</TAG>")
						.build())
				.language(Language.ENGLISH).noContent(false)
				.sortBy(SearchOptions.SortBy.<String, String>field(Beers.FIELD_NAME.getName()).order(Order.ASC))
				.verbatim(false).withSortKeys(true).returnField(Beers.FIELD_NAME.getName())
				.returnField(Beers.FIELD_STYLE_NAME.getName()).build();
		SearchResults<String, String> results = sync.search(Beers.INDEX, "pale", options);
		assertEquals(604, results.getCount());
		Document<String, String> doc1 = results.get(0);
		assertNotNull(doc1.get(Beers.FIELD_NAME.getName()));
		assertNotNull(doc1.get(Beers.FIELD_STYLE_NAME.getName()));
		assertNull(doc1.get(Beers.FIELD_ABV.getName()));

	}

	@ParameterizedTest
	@RedisTestContextsSource
	void search(RedisTestContext context) throws Exception {
		int count = Beers.populateIndex(context.getConnection());
		RedisModulesCommands<String, String> sync = context.sync();
		SearchResults<String, String> results = sync.search(Beers.INDEX, "German");
		assertEquals(163, results.getCount());
		results = sync.search(Beers.INDEX, "Hefeweizen",
				SearchOptions.<String, String>builder().noContent(true).build());
		assertEquals(10, results.size());
		assertTrue(results.get(0).getId().startsWith("beer:"));
		results = sync.search(Beers.INDEX, "Hefeweizen",
				SearchOptions.<String, String>builder().withScores(true).build());
		assertEquals(10, results.size());
		assertTrue(results.get(0).getScore() > 0);
		results = sync.search(Beers.INDEX, "Hefeweizen", SearchOptions.<String, String>builder().withScores(true)
				.noContent(true).limit(new SearchOptions.Limit(0, 100)).build());
		assertEquals(80, results.getCount());
		assertEquals(80, results.size());
		assertTrue(results.get(0).getId().startsWith(Beers.PREFIX));
		assertTrue(results.get(0).getScore() > 0);

		results = sync.search(Beers.INDEX, "pale", SearchOptions.<String, String>builder().withPayloads(true).build());
		assertEquals(604, results.getCount());
		Document<String, String> result1 = results.get(0);
		assertNotNull(result1.get(Beers.FIELD_NAME.getName()));
		assertNotNull(result1.getPayload());
		assertEquals(sync.hget(result1.getId(), Beers.FIELD_DESCRIPTION.getName()), result1.getPayload());

		results = sync.search(Beers.INDEX, "pale", SearchOptions.<String, String>builder()
				.returnField(Beers.FIELD_NAME.getName()).returnField(Beers.FIELD_STYLE_NAME.getName()).build());
		assertEquals(604, results.getCount());
		result1 = results.get(0);
		assertNotNull(result1.get(Beers.FIELD_NAME.getName()));
		assertNotNull(result1.get(Beers.FIELD_STYLE_NAME.getName()));
		assertNull(result1.get(Beers.FIELD_ABV.getName()));

		results = sync.search(Beers.INDEX, "pale",
				SearchOptions.<String, String>builder().returnField(Beers.FIELD_NAME.getName())
						.returnField(Beers.FIELD_STYLE_NAME.getName()).returnField("").build());
		assertEquals(604, results.getCount());
		result1 = results.get(0);
		assertNotNull(result1.get(Beers.FIELD_NAME.getName()));
		assertNotNull(result1.get(Beers.FIELD_STYLE_NAME.getName()));
		assertNull(result1.get(Beers.FIELD_ABV.getName()));

		results = sync.search(Beers.INDEX, "*",
				SearchOptions.<String, String>builder().inKeys("beer:1018", "beer:2428").build());
		assertEquals(2, results.getCount());
		result1 = results.get(0);
		assertNotNull(result1.get(Beers.FIELD_NAME.getName()));
		assertNotNull(result1.get(Beers.FIELD_STYLE_NAME.getName()));
		assertEquals("5.5", result1.get(Beers.FIELD_ABV.getName()));

		results = sync.search(Beers.INDEX, "sculpin",
				SearchOptions.<String, String>builder().inField(Beers.FIELD_NAME.getName()).build());
		assertEquals(1, results.getCount());
		result1 = results.get(0);
		assertNotNull(result1.get(Beers.FIELD_NAME.getName()));
		assertNotNull(result1.get(Beers.FIELD_STYLE_NAME.getName()));
		assertEquals("7", result1.get(Beers.FIELD_ABV.getName()));

		String term = "pale";
		String query = "@style:" + term;
		Tags<String> tags = new Tags<>("<b>", "</b>");
		results = sync.search(Beers.INDEX, query, SearchOptions.<String, String>builder()
				.highlight(SearchOptions.Highlight.<String, String>builder().build()).build());
		for (Document<String, String> result : results) {
			assertTrue(highlighted(result, Beers.FIELD_STYLE_NAME.getName(), tags, term));
		}
		results = sync.search(Beers.INDEX, query,
				SearchOptions.<String, String>builder().highlight(
						SearchOptions.Highlight.<String, String>builder().field(Beers.FIELD_NAME.getName()).build())
						.build());
		for (Document<String, String> result : results) {
			assertFalse(highlighted(result, Beers.FIELD_STYLE_NAME.getName(), tags, term));
		}
		tags = new Tags<>("[start]", "[end]");
		results = sync
				.search(Beers.INDEX, query,
						SearchOptions.<String, String>builder().highlight(SearchOptions.Highlight
								.<String, String>builder().field(Beers.FIELD_STYLE_NAME.getName()).tags(tags).build())
								.build());
		for (Document<String, String> result : results) {
			assertTrue(highlighted(result, Beers.FIELD_STYLE_NAME.getName(), tags, term));
		}

		results = context.reactive()
				.search(Beers.INDEX, "pale",
						SearchOptions.<String, String>builder().limit(new SearchOptions.Limit(200, 100)).build())
				.block();
		assertEquals(604, results.getCount());
		result1 = results.get(0);
		assertNotNull(result1.get(Beers.FIELD_NAME.getName()));
		assertNotNull(result1.get(Beers.FIELD_STYLE_NAME.getName()));
		assertNotNull(result1.get(Beers.FIELD_ABV.getName()));

		results = sync.search(Beers.INDEX, "pail");
		assertEquals(2, results.getCount());

		results = sync.search(Beers.INDEX, "*",
				SearchOptions.<String, String>builder().limit(new SearchOptions.Limit(0, 0)).build());
		assertEquals(count, results.getCount());

		String index = "escapeTagTestIdx";
		String idField = "id";
		context.async().create(index, Field.tag(idField).build()).get();
		Map<String, String> doc1 = new HashMap<>();
		doc1.put(idField, "chris@blah.org,User1#test.org,usersdfl@example.com");
		context.async().hmset("doc1", doc1).get();
		results = context.async().search(index, "@id:{" + RedisModulesUtils.escapeTag("User1#test.org") + "}").get();
		Assertions.assertEquals(1, results.size());
		double minABV = .18;
		double maxABV = 3;
		SearchResults<String, String> filterResults = sync
				.search(Beers.INDEX, "*",
						SearchOptions
								.<String, String>builder().filter(SearchOptions.NumericFilter
										.<String, String>field(Beers.FIELD_ABV.getName()).min(minABV).max(maxABV))
								.build());
		Assertions.assertEquals(4, filterResults.size());
		for (Document<String, String> document : filterResults) {
			double abv = Double.parseDouble(document.get(Beers.FIELD_ABV.getName()));
			Assertions.assertTrue(abv >= minABV);
			Assertions.assertTrue(abv <= maxABV);
		}

	}

	private boolean highlighted(Document<String, String> result, String fieldName, Tags<String> tags, String string) {
		String fieldValue = result.get(fieldName).toLowerCase();
		return fieldValue.contains(tags.getOpen() + string + tags.getClose());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void sugget(RedisTestContext context) throws IOException {
		createBeerSuggestions(context);
		RedisModulesCommands<String, String> sync = context.sync();
		RedisModulesReactiveCommands<String, String> reactive = context.reactive();
		assertEquals(5, sync.sugget(SUGINDEX, "Ame").size());
		assertEquals(5, reactive.sugget(SUGINDEX, "Ame").collectList().block().size());
		SuggetOptions options = SuggetOptions.builder().max(1000L).build();
		assertEquals(8, sync.sugget(SUGINDEX, "Ame", options).size());
		assertEquals(8, reactive.sugget(SUGINDEX, "Ame", options).collectList().block().size());
		Consumer<List<Suggestion<String>>> withScores = results -> {
			assertEquals(7, results.size());
			assertEquals("Ameri-Hefe", results.get(0).getString());
			assertEquals(0.40824830532073975, results.get(0).getScore(), .01);
		};
		SuggetOptions withScoresOptions = SuggetOptions.builder().max(1000L).withScores(true).build();
		withScores.accept(sync.sugget(SUGINDEX, "Ameri", withScoresOptions));
		withScores.accept(reactive.sugget(SUGINDEX, "Ameri", withScoresOptions).collectList().block());
		assertEquals(3816, sync.suglen(SUGINDEX));
		assertTrue(sync.sugdel(SUGINDEX, "Ameri-Hefe"));
		assertTrue(reactive.sugdel(SUGINDEX, "Thunderstorm").block());
	}

	@SuppressWarnings("unchecked")
	@ParameterizedTest
	@RedisTestContextsSource
	void aggregate(RedisTestContext context) throws Exception {
		int count = Beers.populateIndex(context.getConnection());
		MappingIterator<Map<String, Object>> beers = Beers.mapIterator();
		Map<String, Map<String, Object>> beerMap = new HashMap<>();
		while (beers.hasNext()) {
			Map<String, Object> beer = beers.next();
			beerMap.put((String) beer.get(Beers.FIELD_ID.getName()), beer);
		}
		Consumer<AggregateResults<String>> loadAsserts = results -> {
			assertEquals(1, results.getCount());
			assertEquals(count, results.size());
			for (Map<String, Object> result : results) {
				String id = (String) result.get(Beers.FIELD_ID.getName());
				Map<String, Object> beer = beerMap.get(id);
				assertEquals(((String) beer.get(Beers.FIELD_NAME.getName())).toLowerCase(),
						((String) result.get(Beers.FIELD_NAME.getName())).toLowerCase());
				String style = (String) beer.get(Beers.FIELD_STYLE_NAME.getName());
				if (style != null) {
					assertEquals(style.toLowerCase(),
							((String) result.get(Beers.FIELD_STYLE_NAME.getName())).toLowerCase());
				}
			}

		};
		RedisModulesCommands<String, String> sync = context.sync();
		RedisModulesReactiveCommands<String, String> reactive = context.reactive();
		AggregateOptions<String, String> loadOptions = AggregateOptions.<String, String>builder()
				.load(Beers.FIELD_ID.getName()).load(Beers.FIELD_NAME.getName()).load(Beers.FIELD_STYLE_NAME.getName())
				.build();
		loadAsserts.accept(sync.aggregate(Beers.INDEX, "*", loadOptions));
		loadAsserts.accept(reactive.aggregate(Beers.INDEX, "*", loadOptions).block());

		// GroupBy tests
		Consumer<AggregateResults<String>> groupByAsserts = results -> {
			assertEquals(70, results.getCount());
			List<Double> abvs = results.stream().map(r -> Double.parseDouble((String) r.get(Beers.FIELD_ABV.getName())))
					.collect(Collectors.toList());
			assertTrue(abvs.get(0) > abvs.get(abvs.size() - 1));
			assertEquals(20, results.size());
		};
		AggregateOptions<String, String> groupByOptions = AggregateOptions
				.<String, String>group(GroupOperation.property(Beers.FIELD_STYLE_NAME.getName())
						.avg(Avg.property(Beers.FIELD_ABV.getName()).as(Beers.FIELD_ABV.getName()).build()).build())
				.sort(SortOperation.property(SortOperation.Property.name(Beers.FIELD_ABV.getName()).order(Order.DESC))
						.build())
				.limit(LimitOperation.offset(0).num(20)).build();
		groupByAsserts.accept(sync.aggregate(Beers.INDEX, "*", groupByOptions));
		groupByAsserts.accept(reactive.aggregate(Beers.INDEX, "*", groupByOptions).block());

		Consumer<AggregateResults<String>> groupBy0Asserts = results -> {
			assertEquals(1, results.getCount());
			assertEquals(1, results.size());
			Double maxAbv = Double.parseDouble((String) results.get(0).get(Beers.FIELD_ABV.getName()));
			assertEquals(99.9899978638, maxAbv);
		};

		AggregateOptions<String, String> groupBy0Options = AggregateOptions
				.<String, String>group(GroupOperation
						.max(Max.property(Beers.FIELD_ABV.getName()).as(Beers.FIELD_ABV.getName()).build()).build())
				.build();
		groupBy0Asserts.accept(sync.aggregate(Beers.INDEX, "*", groupBy0Options));
		groupBy0Asserts.accept(reactive.aggregate(Beers.INDEX, "*", groupBy0Options).block());

		Consumer<AggregateResults<String>> groupBy2Asserts = results -> {
			assertEquals(70, results.getCount());
			assertEquals("bamberg-style bock rauchbier",
					((String) results.get(0).get(Beers.FIELD_STYLE_NAME.getName())).toLowerCase());
			Object names = results.get(0).get("names");
			assertEquals(1, ((List<String>) names).size());
		};
		GroupOperation group = GroupOperation.property(Beers.FIELD_STYLE_NAME.getName())
				.toList(ToList.property(Beers.FIELD_NAME.getName()).as("names").build()).count(Count.as("count"))
				.build();
		AggregateOptions<String, String> groupBy2Options = AggregateOptions.<String, String>group(group)
				.limit(LimitOperation.offset(0).num(1)).build();
		groupBy2Asserts.accept(sync.aggregate(Beers.INDEX, "*", groupBy2Options));
		groupBy2Asserts.accept(reactive.aggregate(Beers.INDEX, "*", groupBy2Options).block());

		// Cursor tests
		Consumer<AggregateWithCursorResults<String>> cursorTests = cursorResults -> {
			assertEquals(1, cursorResults.getCount());
			assertEquals(1000, cursorResults.size());
//            assertEquals("harpoon ipa (2010)", ((String) cursorResults.get(999).get("name")).toLowerCase());
			assertTrue(((String) cursorResults.get(9).get(Beers.FIELD_ABV.getName())).length() > 0);
		};
		AggregateOptions<String, String> cursorOptions = AggregateOptions.<String, String>builder()
				.load(Beers.FIELD_ID.getName()).load(Beers.FIELD_NAME.getName()).load(Beers.FIELD_ABV.getName())
				.build();
		AggregateWithCursorResults<String> cursorResults = sync.aggregate(Beers.INDEX, "*", new Cursor(),
				cursorOptions);
		cursorTests.accept(cursorResults);
		cursorTests.accept(reactive.aggregate(Beers.INDEX, "*", new Cursor(), cursorOptions).block());
		cursorResults = sync.cursorRead(Beers.INDEX, cursorResults.getCursor(), 500);
		assertEquals(500, cursorResults.size());
		cursorResults = reactive.cursorRead(Beers.INDEX, cursorResults.getCursor()).block();
		assertEquals(500, cursorResults.size());
		String deleteStatus = sync.cursorDelete(Beers.INDEX, cursorResults.getCursor());
		assertEquals("OK", deleteStatus);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void alias(RedisTestContext context) throws Exception {

		Beers.populateIndex(context.getConnection());
		// SYNC

		String alias = "alias123";

		RedisModulesCommands<String, String> sync = context.sync();
		sync.aliasadd(alias, Beers.INDEX);
		SearchResults<String, String> results = sync.search(alias, "*");
		assertTrue(results.size() > 0);

		String newAlias = "alias456";
		sync.aliasupdate(newAlias, Beers.INDEX);
		assertTrue(sync.search(newAlias, "*").size() > 0);

		sync.aliasdel(newAlias);
		try {
			sync.search(newAlias, "*");
			fail("Alias was not removed");
		} catch (RedisCommandExecutionException e) {
			assertTrue(e.getMessage().contains("no such index") || e.getMessage().contains("Unknown Index name"));
		}

		sync.aliasdel(alias);
		RedisModulesAsyncCommands<String, String> async = context.async();
		// ASYNC
		async.aliasadd(alias, Beers.INDEX).get();
		results = async.search(alias, "*").get();
		assertTrue(results.size() > 0);

		async.aliasupdate(newAlias, Beers.INDEX).get();
		assertTrue(async.search(newAlias, "*").get().size() > 0);

		async.aliasdel(newAlias).get();
		try {
			async.search(newAlias, "*").get();
			fail("Alias was not removed");
		} catch (ExecutionException e) {
			assertTrue(e.getCause().getMessage().contains("no such index")
					|| e.getCause().getMessage().contains("Unknown Index name"));
		}

		sync.aliasdel(alias);

		RedisModulesReactiveCommands<String, String> reactive = context.reactive();
		// REACTIVE
		reactive.aliasadd(alias, Beers.INDEX).block();
		results = reactive.search(alias, "*").block();
		assertTrue(results.size() > 0);

		reactive.aliasupdate(newAlias, Beers.INDEX).block();
		results = reactive.search(newAlias, "*").block();
		Assertions.assertFalse(results.isEmpty());

		reactive.aliasdel(newAlias).block();
		try {
			reactive.search(newAlias, "*").block();
			fail("Alias was not removed");
		} catch (RedisCommandExecutionException e) {
			assertTrue(e.getMessage().contains("no such index") || e.getMessage().contains("Unknown Index name"));
		}

	}

	@ParameterizedTest
	@RedisTestContextsSource
	void info(RedisTestContext context) throws Exception {
		int count = Beers.populateIndex(context.getConnection());
		List<Object> infoList = context.async().indexInfo(Beers.INDEX).get();
		IndexInfo info = RedisModulesUtils.indexInfo(infoList);
		Assertions.assertEquals(count, info.getNumDocs());
		List<Field> fields = info.getFields();
		Field.Text descriptionField = (Field.Text) fields.get(5);
		Assertions.assertEquals(Beers.FIELD_DESCRIPTION.getName(), descriptionField.getName());
		Assertions.assertFalse(descriptionField.getOptions().isNoIndex());
		Assertions.assertFalse(descriptionField.isNoStem());
		Assertions.assertFalse(descriptionField.getOptions().isSortable());
		Field.Tag styleField = (Field.Tag) fields.get(2);
		Assertions.assertEquals(Beers.FIELD_STYLE_NAME.getName(), styleField.getName());
		Assertions.assertTrue(styleField.getOptions().isSortable());
		Assertions.assertEquals(",", styleField.getSeparator());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void tagVals(RedisTestContext context) throws Exception {
		Beers.populateIndex(context.getConnection());
		Set<String> TAG_VALS = new HashSet<>(Arrays.asList("ordinary bitter", "german-style heller bock/maibock",
				"traditional german-style bock", "smoke beer", "baltic-style porter", "specialty honey lager or ale",
				"oatmeal stout", "imperial or double red ale", "german-style schwarzbier",
				"classic english-style pale ale", "old ale", "english-style dark mild ale", "belgian-style tripel",
				"strong ale", "classic irish-style dry stout", "belgian-style dubbel", "porter",
				"belgian-style fruit lambic", "german-style brown ale/altbier", "american-style strong pale ale",
				"golden or blonde ale", "belgian-style quadrupel", "american-style imperial stout",
				"belgian-style pale strong ale", "english-style pale mild ale", "sweet stout",
				"american-style pale ale", "irish-style red ale", "dark american-belgo-style ale",
				"german-style pilsener", "english-style india pale ale", "french & belgian-style saison",
				"american-style brown ale", "out of category", "vienna-style lager",
				"american-style cream ale or lager", "american rye ale or lager", "fruit beer", "pumpkin beer",
				"foreign (export)-style stout", "american-style light lager", "american-style india pale ale",
				"german-style oktoberfest", "european low-alcohol lager", "other belgian-style ales",
				"american-style stout", "specialty beer", "belgian-style dark strong ale", "winter warmer",
				"american-style lager", "american-style barley wine ale", "scottish-style light ale",
				"herb and spice beer", "south german-style hefeweizen", "hops grown in the great pacific northwest",
				"imperial or double india pale ale", "american-style dark lager", "special bitter or best bitter",
				"american-style india black ale", "light american wheat ale or lager", "american-style amber/red ale",
				"scotch ale", "german-style doppelbock", "bamberg-style bock rauchbier", "extra special bitter",
				"south german-style weizenbock", "belgian-style white", "belgian-style pale ale", "kellerbier - ale"));
		HashSet<String> actual = new HashSet<>(context.sync().tagvals(Beers.INDEX, Beers.FIELD_STYLE_NAME.getName()));
		Assertions.assertEquals(TAG_VALS, actual);
		Assertions.assertEquals(TAG_VALS, new HashSet<>(
				context.reactive().tagvals(Beers.INDEX, Beers.FIELD_STYLE_NAME.getName()).collectList().block()));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void emptyToListReducer(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		// FT.CREATE idx ON HASH PREFIX 1 my_prefix: SCHEMA category TAG SORTABLE color
		// TAG SORTABLE size TAG SORTABLE
		sync.create("idx", CreateOptions.<String, String>builder().prefix("my_prefix:").build(),
				Field.tag("category").sortable().build(), Field.tag("color").sortable().build(),
				Field.tag("size").sortable().build());
		Map<String, String> doc1 = mapOf("category", "31", "color", "red");
		sync.hset("my_prefix:1", doc1);
		AggregateOptions<String, String> aggregateOptions = AggregateOptions.<String, String>group(GroupOperation
				.property("category")
				.reducers(ToList.property("color").as("color").build(), ToList.property("size").as("size").build())
				.build()).build();
		AggregateResults<String> results = sync.aggregate("idx", "@color:{red|blue}", aggregateOptions);
		Assertions.assertEquals(1, results.size());
		Map<String, Object> expectedResult = new HashMap<>();
		expectedResult.put("category", "31");
		expectedResult.put("color", Collections.singletonList("red"));
		expectedResult.put("size", Collections.emptyList());
		Assertions.assertEquals(expectedResult, results.get(0));
	}

	final static String[] DICT_TERMS = new String[] { "beer", "ale", "brew", "brewski" };

	@ParameterizedTest
	@RedisTestContextsSource
	void dictadd(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		Assertions.assertEquals(DICT_TERMS.length, sync.dictadd("beers", DICT_TERMS));
		Assertions.assertEquals(new HashSet<>(Arrays.asList(DICT_TERMS)), new HashSet<>(sync.dictdump("beers")));
		Assertions.assertEquals(1, sync.dictdel("beers", "brew"));
		List<String> beerDict = new ArrayList<>();
		Collections.addAll(beerDict, DICT_TERMS);
		beerDict.remove("brew");
		Assertions.assertEquals(new HashSet<>(beerDict), new HashSet<>(sync.dictdump("beers")));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void dictaddReactive(RedisTestContext context) {
		RedisModulesReactiveCommands<String, String> reactive = context.reactive();
		Assertions.assertEquals(DICT_TERMS.length, reactive.dictadd("beers", DICT_TERMS).block());
		Assertions.assertEquals(new HashSet<>(Arrays.asList(DICT_TERMS)),
				new HashSet<>(reactive.dictdump("beers").collectList().block()));
		Assertions.assertEquals(1, reactive.dictdel("beers", "brew").block());
		List<String> beerDict = new ArrayList<>();
		Collections.addAll(beerDict, DICT_TERMS);
		beerDict.remove("brew");
		Assertions.assertEquals(new HashSet<>(beerDict),
				new HashSet<>(reactive.dictdump("beers").collectList().block()));
	}

}
