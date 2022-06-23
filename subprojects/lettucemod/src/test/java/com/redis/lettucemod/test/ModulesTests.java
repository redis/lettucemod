package com.redis.lettucemod.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.unit.DataSize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.RedisModulesUtils;
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
import com.redis.lettucemod.output.ExecutionResults;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateOptions.Load;
import com.redis.lettucemod.search.AggregateResults;
import com.redis.lettucemod.search.AggregateWithCursorResults;
import com.redis.lettucemod.search.CreateOptions;
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
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.lettuce.core.KeyValue;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;
import reactor.core.publisher.Mono;

class ModulesTests extends AbstractTestcontainersRedisTestBase {

	private static final Logger log = LoggerFactory.getLogger(ModulesTests.class);

	@SuppressWarnings("resource")
	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(
				new RedisModulesContainer(
						RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG)),
				new RedisEnterpriseContainer(
						RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag(RedisEnterpriseContainer.DEFAULT_TAG))
						.withDatabase(Database.name("ModulesTests").memory(DataSize.ofMegabytes(300)).ossCluster(true)
								.modules(RedisModule.SEARCH, RedisModule.JSON, RedisModule.GEARS,
										RedisModule.TIMESERIES)
								.build()));
	}

	protected static Map<String, String> mapOf(String... keyValues) {
		Map<String, String> map = new HashMap<>();
		for (int index = 0; index < keyValues.length / 2; index++) {
			map.put(keyValues[index * 2], keyValues[index * 2 + 1]);
		}
		return map;
	}

	protected final static String SUGINDEX = "beersSug";

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

	@ParameterizedTest
	@RedisTestContextsSource
	void client(RedisTestContext context) {
		if (context.isCluster()) {
			ping(RedisModulesClusterClient.create(context.getRedisURI()).connect());
			ping(RedisModulesClusterClient
					.create(DefaultClientResources.create(), RedisURI.create(context.getRedisURI())).connect());
			ping(RedisModulesClusterClient.create(context.getRedisURI()).connect(StringCodec.UTF8));
		} else {
			ping(RedisModulesClient.create().connect(RedisURI.create(context.getRedisURI())));
			ping(RedisModulesClient.create(DefaultClientResources.create())
					.connect(RedisURI.create(context.getRedisURI())));
			ping(RedisModulesClient.create(DefaultClientResources.create(), context.getRedisURI()).connect());
			ping(RedisModulesClient.create(DefaultClientResources.create(), RedisURI.create(context.getRedisURI()))
					.connect());
			ping(RedisModulesClient.create().connect(StringCodec.UTF8, RedisURI.create(context.getRedisURI())));
		}
	}

	private void ping(StatefulRedisModulesConnection<String, String> connection) {
		assertEquals("PONG", connection.reactive().ping().block());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void ftSugaddIncr(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		String key = "testSugadd";
		sync.sugadd(key, "value1", 1);
		sync.sugaddIncr(key, "value1", 1);
		List<Suggestion<String>> suggestions = sync.sugget(key, "value",
				SuggetOptions.builder().withScores(true).build());
		assertEquals(1, suggestions.size());
		assertEquals(1.4142135381698608, suggestions.get(0).getScore());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void ftSugaddPayload(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		String key = "testSugadd";
		sync.sugadd(key, "value1", 1, "somepayload");
		List<Suggestion<String>> suggestions = sync.sugget(key, "value",
				SuggetOptions.builder().withPayloads(true).build());
		assertEquals(1, suggestions.size());
		assertEquals("somepayload", suggestions.get(0).getPayload());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void ftSugaddScorePayload(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		String key = "testSugadd";
		sync.sugadd(key, "value1", 2, "somepayload");
		List<Suggestion<String>> suggestions = sync.sugget(key, "value",
				SuggetOptions.builder().withScores(true).withPayloads(true).build());
		assertEquals(1, suggestions.size());
		assertEquals(1.4142135381698608, suggestions.get(0).getScore());
		assertEquals("somepayload", suggestions.get(0).getPayload());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void ftCreate(RedisTestContext context) throws Exception {
		int count = Beers.populateIndex(context.getConnection());
		RedisModulesCommands<String, String> sync = context.sync();
		assertEquals(count, RedisModulesUtils.indexInfo(sync.indexInfo(Beers.INDEX)).getNumDocs());
		CreateOptions<String, String> options = CreateOptions.<String, String>builder().prefix("release:")
				.payloadField("xml").build();
		Field[] fields = new Field[] { Field.text("artist").sortable().build(), Field.tag("id").sortable().build(),
				Field.text("title").sortable().build() };
		sync.create("releases", options, fields);
		assertEquals(fields.length, RedisModulesUtils.indexInfo(sync.indexInfo("releases")).getFields().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void ftCreateTemporaryIndex(RedisTestContext context) throws Exception {
		Beers.populateIndex(context.getConnection());
		RedisModulesCommands<String, String> sync = context.sync();
		String tempIndex = "temporaryIndex";
		sync.create(tempIndex, CreateOptions.<String, String>builder().temporary(1L).build(),
				Field.text("field1").build());
		assertEquals(tempIndex, RedisModulesUtils.indexInfo(sync.indexInfo(tempIndex)).getIndexName());
		Awaitility.await().until(() -> !sync.list().contains(tempIndex));
		Assertions.assertThrows(RedisCommandExecutionException.class, () -> sync.indexInfo(tempIndex),
				"Unknown Index name");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void ftAlterIndex(RedisTestContext context) throws Exception {
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
	void ftDropindexDeleteDocs(RedisTestContext context) throws Exception {
		Beers.populateIndex(context.getConnection());
		RedisModulesCommands<String, String> sync = context.sync();
		sync.dropindexDeleteDocs(Beers.INDEX);
		Awaitility.await().until(() -> sync.list().isEmpty());
		Assertions.assertThrows(RedisCommandExecutionException.class, () -> sync.indexInfo(Beers.INDEX));
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
	void ftList(RedisTestContext context) throws ExecutionException, InterruptedException {
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
	void ftSearchOptions(RedisTestContext context) throws IOException {
		Beers.populateIndex(context.getConnection());
		RedisModulesCommands<String, String> sync = context.sync();
		SearchOptions<String, String> options = SearchOptions.<String, String>builder().withPayloads(true)
				.noStopWords(true).limit(SearchOptions.limit(10, 100)).withScores(true)
				.highlight(Highlight.<String, String>builder().field(Beers.FIELD_NAME.getName()).tags("<TAG>", "</TAG>")
						.build())
				.language(Language.ENGLISH).noContent(false)
				.sortBy(SearchOptions.SortBy.asc(Beers.FIELD_NAME.getName())).verbatim(false).withSortKeys(true)
				.returnField(Beers.FIELD_NAME.getName()).returnField(Beers.FIELD_STYLE_NAME.getName()).build();
		SearchResults<String, String> results = sync.search(Beers.INDEX, "pale", options);
		assertEquals(710, results.getCount());
		Document<String, String> doc1 = results.get(0);
		assertNotNull(doc1.get(Beers.FIELD_NAME.getName()));
		assertNotNull(doc1.get(Beers.FIELD_STYLE_NAME.getName()));
		assertNull(doc1.get(Beers.FIELD_ABV.getName()));

	}

	private void assertSearch(RedisTestContext context, String query, SearchOptions<String, String> options,
			long expectedCount, String... expectedAbv) {
		SearchResults<String, String> results = context.sync().search(Beers.INDEX, query, options);
		assertEquals(expectedCount, results.getCount());
		Document<String, String> doc1 = results.get(0);
		assertNotNull(doc1.get(Beers.FIELD_NAME.getName()));
		assertNotNull(doc1.get(Beers.FIELD_STYLE_NAME.getName()));
		if (expectedAbv.length > 0) {
			assertTrue(Arrays.asList(expectedAbv).contains(doc1.get(Beers.FIELD_ABV.getName())));
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void ftSearch(RedisTestContext context) throws Exception {
		Beers.populateIndex(context.getConnection());
		RedisModulesCommands<String, String> sync = context.sync();
		SearchResults<String, String> results = sync.search(Beers.INDEX, "German");
		assertEquals(193, results.getCount());
		results = sync.search(Beers.INDEX, "Hefeweizen",
				SearchOptions.<String, String>builder().noContent(true).build());
		assertEquals(10, results.size());
		assertTrue(results.get(0).getId().startsWith("beer:"));
		results = sync.search(Beers.INDEX, "Hefeweizen",
				SearchOptions.<String, String>builder().withScores(true).build());
		assertEquals(10, results.size());
		assertTrue(results.get(0).getScore() > 0);
		results = sync.search(Beers.INDEX, "Hefeweizen", SearchOptions.<String, String>builder().withScores(true)
				.noContent(true).limit(new Limit(0, 100)).build());
		assertEquals(81, results.getCount());
		assertEquals(81, results.size());
		assertTrue(results.get(0).getId().startsWith(Beers.PREFIX));
		assertTrue(results.get(0).getScore() > 0);

		results = sync.search(Beers.INDEX, "pale", SearchOptions.<String, String>builder().withPayloads(true).build());
		assertEquals(710, results.getCount());
		Document<String, String> result1 = results.get(0);
		assertNotNull(result1.get(Beers.FIELD_NAME.getName()));
		assertEquals(result1.get(Beers.FIELD_DESCRIPTION.getName()), result1.getPayload());
		assertEquals(sync.hget(result1.getId(), Beers.FIELD_DESCRIPTION.getName()), result1.getPayload());

		assertSearch(context, "pale", SearchOptions.<String, String>builder().returnField(Beers.FIELD_NAME.getName())
				.returnField(Beers.FIELD_STYLE_NAME.getName()).build(), 710);
		assertSearch(context, "pale", SearchOptions.<String, String>builder().returnField(Beers.FIELD_NAME.getName())
				.returnField(Beers.FIELD_STYLE_NAME.getName()).returnField("").build(), 710);
		assertSearch(context, "*", SearchOptions.<String, String>builder().inKeys("beer:1018", "beer:2428").build(), 2,
				"9", "5.5");
		assertSearch(context, "sculpin",
				SearchOptions.<String, String>builder().inField(Beers.FIELD_NAME.getName()).build(), 1, "7");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void testSearchTags(RedisTestContext context) throws InterruptedException, ExecutionException, IOException {
		int count = Beers.populateIndex(context.getConnection());
		RedisModulesCommands<String, String> sync = context.sync();
		String term = "pale";
		String query = "@style:" + term;
		Tags<String> tags = new Tags<>("<b>", "</b>");
		SearchResults<String, String> results = sync.search(Beers.INDEX, query, SearchOptions.<String, String>builder()
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
				.search(Beers.INDEX, "pale", SearchOptions.<String, String>builder().limit(new Limit(200, 100)).build())
				.block();
		assertEquals(710, results.getCount());
		Document<String, String> result1 = results.get(0);
		assertNotNull(result1.get(Beers.FIELD_NAME.getName()));
		assertNotNull(result1.get(Beers.FIELD_STYLE_NAME.getName()));
		assertNotNull(result1.get(Beers.FIELD_ABV.getName()));

		results = sync.search(Beers.INDEX, "pail");
		assertEquals(417, results.getCount());

		results = sync.search(Beers.INDEX, "*", SearchOptions.<String, String>builder().limit(new Limit(0, 0)).build());
		assertEquals(count, results.getCount());

		String index = "escapeTagTestIdx";
		String idField = "id";
		context.async().create(index, Field.tag(idField).build()).get();
		Map<String, String> doc1 = new HashMap<>();
		doc1.put(idField, "chris@blah.org,User1#test.org,usersdfl@example.com");
		context.async().hmset("doc1", doc1).get();
		results = context.async().search(index, "@id:{" + RedisModulesUtils.escapeTag("User1#test.org") + "}").get();
		assertEquals(1, results.size());
		double minABV = .18;
		double maxABV = 3;
		SearchResults<String, String> filterResults = sync
				.search(Beers.INDEX, "*",
						SearchOptions
								.<String, String>builder().filter(SearchOptions.NumericFilter
										.<String, String>field(Beers.FIELD_ABV.getName()).min(minABV).max(maxABV))
								.build());
		assertEquals(4, filterResults.size());
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
	void ftSugget(RedisTestContext context) throws IOException {
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
	void ftAggregate(RedisTestContext context) throws Exception {
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
				String style = (String) beer.get("style");
				if (style != null) {
					assertEquals(style.toLowerCase(), ((String) result.get("style")).toLowerCase());
				}
			}

		};
		RedisModulesCommands<String, String> sync = context.sync();
		RedisModulesReactiveCommands<String, String> reactive = context.reactive();
		AggregateOptions<String, String> loadOptions = AggregateOptions.<String, String>builder()
				.load(Beers.FIELD_ID.getName()).load(Load.identifier(Beers.FIELD_NAME.getName()).build())
				.load(Load.identifier(Beers.FIELD_STYLE_NAME.getName()).as("style").build()).build();
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
				.<String, String>group(Group.by(Beers.FIELD_STYLE_NAME.getName())
						.avg(Avg.property(Beers.FIELD_ABV.getName()).as(Beers.FIELD_ABV.getName()).build()).build())
				.sort(Sort.by(Sort.Property.desc(Beers.FIELD_ABV.getName())).build()).limit(Limit.offset(0).num(20))
				.build();
		groupByAsserts.accept(sync.aggregate(Beers.INDEX, "*", groupByOptions));
		groupByAsserts.accept(reactive.aggregate(Beers.INDEX, "*", groupByOptions).block());

		Consumer<AggregateResults<String>> groupBy0Asserts = results -> {
			assertEquals(1, results.getCount());
			assertEquals(1, results.size());
			Double maxAbv = Double.parseDouble((String) results.get(0).get(Beers.FIELD_ABV.getName()));
			assertEquals(99.9899978638, maxAbv);
		};

		AggregateOptions<String, String> groupBy0Options = AggregateOptions
				.<String, String>group(Group.by()
						.max(Max.property(Beers.FIELD_ABV.getName()).as(Beers.FIELD_ABV.getName()).build()).build())
				.build();
		groupBy0Asserts.accept(sync.aggregate(Beers.INDEX, "*", groupBy0Options));
		groupBy0Asserts.accept(reactive.aggregate(Beers.INDEX, "*", groupBy0Options).block());

		Consumer<AggregateResults<String>> groupBy2Asserts = results -> {
			assertEquals(70, results.getCount());
			Map<String, Object> doc = results.get(1);
			Assumptions.assumeTrue(doc != null);
			Assumptions.assumeTrue(doc.get(Beers.FIELD_STYLE_NAME.getName()) != null);
			String style = ((String) doc.get(Beers.FIELD_STYLE_NAME.getName())).toLowerCase();
			assertTrue(style.equals("bamberg-style bock rauchbier") || style.equals("south german-style hefeweizen"));
			int nameCount = ((List<String>) results.get(1).get("names")).size();
			assertTrue(nameCount == 1 || nameCount == 141);
		};
		Group group = Group.by(Beers.FIELD_STYLE_NAME.getName())
				.toList(ToList.property(Beers.FIELD_NAME.getName()).as("names").build()).count(Count.as("count"))
				.build();
		AggregateOptions<String, String> groupBy2Options = AggregateOptions.<String, String>group(group)
				.limit(Limit.offset(0).num(2)).build();
		groupBy2Asserts.accept(sync.aggregate(Beers.INDEX, "*", groupBy2Options));
		groupBy2Asserts.accept(reactive.aggregate(Beers.INDEX, "*", groupBy2Options).block());

		// Cursor tests
		Consumer<AggregateWithCursorResults<String>> cursorTests = cursorResults -> {
			assertEquals(1, cursorResults.getCount());
			assertEquals(1000, cursorResults.size());
			assertTrue(((String) cursorResults.get(9).get(Beers.FIELD_ABV.getName())).length() > 0);
		};
		AggregateOptions<String, String> cursorOptions = AggregateOptions.<String, String>builder()
				.load(Beers.FIELD_ID.getName()).load(Beers.FIELD_NAME.getName()).load(Beers.FIELD_ABV.getName())
				.build();
		AggregateWithCursorResults<String> cursorResults = sync.aggregate(Beers.INDEX, "*", new CursorOptions(),
				cursorOptions);
		cursorTests.accept(cursorResults);
		cursorTests.accept(reactive.aggregate(Beers.INDEX, "*", new CursorOptions(), cursorOptions).block());
		cursorResults = sync.cursorRead(Beers.INDEX, cursorResults.getCursor(), 500);
		assertEquals(500, cursorResults.size());
		cursorResults = reactive.cursorRead(Beers.INDEX, cursorResults.getCursor()).block();
		assertEquals(500, cursorResults.size());
		String deleteStatus = sync.cursorDelete(Beers.INDEX, cursorResults.getCursor());
		assertEquals("OK", deleteStatus);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void ftAlias(RedisTestContext context) throws Exception {

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
		Assertions.assertThrows(RedisCommandExecutionException.class, () -> sync.search(newAlias, "*"),
				"no such index");

		sync.aliasdel(alias);
		RedisModulesAsyncCommands<String, String> async = context.async();
		// ASYNC
		async.aliasadd(alias, Beers.INDEX).get();
		results = async.search(alias, "*").get();
		assertTrue(results.size() > 0);

		async.aliasupdate(newAlias, Beers.INDEX).get();
		assertTrue(async.search(newAlias, "*").get().size() > 0);

		async.aliasdel(newAlias).get();
		Assertions.assertThrows(ExecutionException.class, () -> async.search(newAlias, "*").get(), "no such index");

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
		Mono<SearchResults<String, String>> searchResults = reactive.search(newAlias, "*");
		Assertions.assertThrows(RedisCommandExecutionException.class, () -> searchResults.block(), "no such index");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void ftInfo(RedisTestContext context) throws Exception {
		int count = Beers.populateIndex(context.getConnection());
		List<Object> infoList = context.async().indexInfo(Beers.INDEX).get();
		IndexInfo info = RedisModulesUtils.indexInfo(infoList);
		assertEquals(count, info.getNumDocs());
		List<Field> fields = info.getFields();
		Field.TextField descriptionField = (Field.TextField) fields.get(5);
		assertEquals(Beers.FIELD_DESCRIPTION.getName(), descriptionField.getName());
		Assertions.assertFalse(descriptionField.isNoIndex());
		Assertions.assertTrue(descriptionField.isNoStem());
		Assertions.assertFalse(descriptionField.isSortable());
		Field.TagField styleField = (Field.TagField) fields.get(2);
		assertEquals(Beers.FIELD_STYLE_NAME.getName(), styleField.getName());
		Assertions.assertTrue(styleField.isSortable());
		assertEquals(",", styleField.getSeparator().get());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void ftTagVals(RedisTestContext context) throws Exception {
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
		assertEquals(TAG_VALS, actual);
		assertEquals(TAG_VALS, new HashSet<>(
				context.reactive().tagvals(Beers.INDEX, Beers.FIELD_STYLE_NAME.getName()).collectList().block()));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void ftEmptyToListReducer(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		// FT.CREATE idx ON HASH PREFIX 1 my_prefix: SCHEMA category TAG SORTABLE color
		// TAG SORTABLE size TAG SORTABLE
		sync.create("idx", CreateOptions.<String, String>builder().prefix("my_prefix:").build(),
				Field.tag("category").sortable().build(), Field.tag("color").sortable().build(),
				Field.tag("size").sortable().build());
		Map<String, String> doc1 = mapOf("category", "31", "color", "red");
		sync.hset("my_prefix:1", doc1);
		AggregateOptions<String, String> aggregateOptions = AggregateOptions.<String, String>group(Group.by("category")
				.reducers(ToList.property("color").as("color").build(), ToList.property("size").as("size").build())
				.build()).build();
		AggregateResults<String> results = sync.aggregate("idx", "@color:{red|blue}", aggregateOptions);
		assertEquals(1, results.size());
		Map<String, Object> expectedResult = new HashMap<>();
		expectedResult.put("category", "31");
		expectedResult.put("color", Collections.singletonList("red"));
		expectedResult.put("size", Collections.emptyList());
		assertEquals(expectedResult, results.get(0));
	}

	final static String[] DICT_TERMS = new String[] { "beer", "ale", "brew", "brewski" };

	@ParameterizedTest
	@RedisTestContextsSource
	void ftDictadd(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		assertEquals(DICT_TERMS.length, sync.dictadd("beers", DICT_TERMS));
		assertEquals(new HashSet<>(Arrays.asList(DICT_TERMS)), new HashSet<>(sync.dictdump("beers")));
		assertEquals(1, sync.dictdel("beers", "brew"));
		List<String> beerDict = new ArrayList<>();
		Collections.addAll(beerDict, DICT_TERMS);
		beerDict.remove("brew");
		assertEquals(new HashSet<>(beerDict), new HashSet<>(sync.dictdump("beers")));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void ftDictaddReactive(RedisTestContext context) {
		RedisModulesReactiveCommands<String, String> reactive = context.reactive();
		assertEquals(DICT_TERMS.length, reactive.dictadd("beers", DICT_TERMS).block());
		assertEquals(new HashSet<>(Arrays.asList(DICT_TERMS)),
				new HashSet<>(reactive.dictdump("beers").collectList().block()));
		assertEquals(1, reactive.dictdel("beers", "brew").block());
		List<String> beerDict = new ArrayList<>();
		Collections.addAll(beerDict, DICT_TERMS);
		beerDict.remove("brew");
		assertEquals(new HashSet<>(beerDict), new HashSet<>(reactive.dictdump("beers").collectList().block()));
	}

	private static final String LABEL_SENSOR_ID = "sensor_id";
	private static final String LABEL_AREA_ID = "area_id";
	private static final long TIMESTAMP_1 = 1548149181;
	private static final long TIMESTAMP_2 = 1548149191;
	private static final double VALUE_1 = 30;
	private static final double VALUE_2 = 42;
	private static final String KEY = "temperature:3:11";
	private static final String SENSOR_ID = "2";
	private static final String AREA_ID = "32";
	private static final String FILTER = LABEL_SENSOR_ID + "=" + SENSOR_ID;

	@SuppressWarnings("unchecked")
	@ParameterizedTest
	@RedisTestContextsSource
	void tsCreate(RedisTestContext context) {
		String status = context.sync().create(KEY,
				com.redis.lettucemod.timeseries.CreateOptions.<String, String>builder().retentionPeriod(6000).build());
		assertEquals("OK", status);
		assertEquals("OK",
				context.sync().create("virag",
						com.redis.lettucemod.timeseries.CreateOptions.<String, String>builder().retentionPeriod(100000L)
								.labels(Label.of("name", "value")).policy(DuplicatePolicy.LAST).build()));
	}

	@SuppressWarnings("unchecked")
	@ParameterizedTest
	@RedisTestContextsSource
	void tsAdd(RedisTestContext context) {
		RedisTimeSeriesCommands<String, String> ts = context.sync();
		// TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
		// TS.ADD temperature:3:11 1548149181 30
		Long add1 = ts.add(KEY, TIMESTAMP_1, VALUE_1, AddOptions.<String, String>builder().retentionPeriod(6000)
				.labels(Label.of(LABEL_SENSOR_ID, SENSOR_ID), Label.of(LABEL_AREA_ID, AREA_ID)).build());
		assertEquals(TIMESTAMP_1, add1);
		List<GetResult<String, String>> results = ts.tsMget(FILTER);
		assertEquals(1, results.size());
		assertEquals(TIMESTAMP_1, results.get(0).getSample().getTimestamp());
		// TS.ADD temperature:3:11 1548149191 42
		Long add2 = ts.add(KEY, TIMESTAMP_2, VALUE_2);
		assertEquals(TIMESTAMP_2, add2);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void tsRange(RedisTestContext context) {
		RedisTimeSeriesCommands<String, String> ts = context.sync();
		populateTimeSeries(ts);
		assertTSRange(ts.range(KEY, RangeOptions.range(TIMESTAMP_1 - 10, TIMESTAMP_2 + 10)
				.aggregation(Aggregation.builder(Aggregation.Aggregator.AVG, 5).build()).build()));
		assertTSRange(ts.range(KEY,
				RangeOptions.all().aggregation(Aggregation.builder(Aggregation.Aggregator.AVG, 5).build()).build()));
		assertTSRange(ts.range(KEY, RangeOptions.from(TIMESTAMP_1 - 10)
				.aggregation(Aggregation.builder(Aggregation.Aggregator.AVG, 5).build()).build()));
		assertTSRange(ts.range(KEY, RangeOptions.to(TIMESTAMP_2 + 10)
				.aggregation(Aggregation.builder(Aggregation.Aggregator.AVG, 5).build()).build()));
	}

	private void assertTSRange(List<Sample> results) {
		assertEquals(2, results.size());
		assertEquals(1548149180, results.get(0).getTimestamp());
		assertEquals(VALUE_1, results.get(0).getValue());
		assertEquals(1548149190, results.get(1).getTimestamp());
		assertEquals(VALUE_2, results.get(1).getValue());
	}

	@SuppressWarnings("unchecked")
	private void populateTimeSeries(RedisTimeSeriesCommands<String, String> ts) {
		// TS.CREATE temperature:3:11 RETENTION 6000 LABELS sensor_id 2 area_id 32
		// TS.ADD temperature:3:11 1548149181 30
		ts.add(KEY, Sample.of(TIMESTAMP_1, VALUE_1), AddOptions.<String, String>builder().retentionPeriod(6000)
				.labels(Label.of(LABEL_SENSOR_ID, SENSOR_ID), Label.of(LABEL_AREA_ID, AREA_ID)).build());
		// TS.ADD temperature:3:11 1548149191 42
		ts.add(KEY, TIMESTAMP_2, VALUE_2);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void tsMget(RedisTestContext context) {
		RedisTimeSeriesCommands<String, String> ts = context.sync();
		populateTimeSeries(ts);
		List<GetResult<String, String>> results = ts.tsMget(FILTER);
		Assertions.assertEquals(1, results.size());
		Assertions.assertEquals(KEY, results.get(0).getKey());
		Assertions.assertEquals(TIMESTAMP_2, results.get(0).getSample().getTimestamp());
		Assertions.assertEquals(VALUE_2, results.get(0).getSample().getValue());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void tsGet(RedisTestContext context) {
		RedisTimeSeriesCommands<String, String> ts = context.sync();
		populateTimeSeries(ts);
		Sample result = ts.tsGet(KEY);
		Assertions.assertEquals(TIMESTAMP_2, result.getTimestamp());
		Assertions.assertEquals(VALUE_2, result.getValue());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void tsMrange(RedisTestContext context) {
		RedisTimeSeriesCommands<String, String> ts = context.sync();
		populateTimeSeries(ts);
		assertTSMRange(KEY, ts.mrange(MRangeOptions.<String, String>all().filters(FILTER).build()));
		assertTSMRange(KEY, ts.mrange(MRangeOptions.<String, String>from(TIMESTAMP_1 - 10).filters(FILTER).build()));
		assertTSMRange(KEY, ts.mrange(MRangeOptions.<String, String>to(TIMESTAMP_2 + 10).filters(FILTER).build()));
		List<RangeResult<String, String>> results = ts
				.mrange(MRangeOptions.<String, String>from(0).withLabels().filters(FILTER).build());
		assertEquals(1, results.size());
		assertEquals(KEY, results.get(0).getKey());
		assertEquals(2, results.get(0).getSamples().size());
		assertEquals(TIMESTAMP_1, results.get(0).getSamples().get(0).getTimestamp());
		assertEquals(VALUE_1, results.get(0).getSamples().get(0).getValue());
		assertEquals(TIMESTAMP_2, results.get(0).getSamples().get(1).getTimestamp());
		assertEquals(VALUE_2, results.get(0).getSamples().get(1).getValue());
		assertEquals(2, results.get(0).getLabels().size());
		assertEquals(SENSOR_ID, results.get(0).getLabels().get(LABEL_SENSOR_ID));
		assertEquals(AREA_ID, results.get(0).getLabels().get(LABEL_AREA_ID));
	}

	private void assertTSMRange(String key, List<RangeResult<String, String>> results) {
		assertEquals(1, results.size());
		assertEquals(key, results.get(0).getKey());
		assertEquals(2, results.get(0).getSamples().size());
		assertEquals(TIMESTAMP_1, results.get(0).getSamples().get(0).getTimestamp());
		assertEquals(VALUE_1, results.get(0).getSamples().get(0).getValue());
		assertEquals(TIMESTAMP_2, results.get(0).getSamples().get(1).getTimestamp());
		assertEquals(VALUE_2, results.get(0).getSamples().get(1).getValue());
	}

	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final String JSON = "{\"name\":\"Leonard Cohen\",\"lastSeen\":1478476800,\"loggedOut\": true}";

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonSet(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		String result = sync.jsonSet("obj", ".", JSON);
		assertEquals("OK", result);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonSetNX(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		String result = sync.jsonSet("obj", ".", JSON, SetMode.NX);
		Assertions.assertNull(result);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonSetXX(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		String result = sync.jsonSet("obj", ".", "true", SetMode.XX);
		Assertions.assertNull(result);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonGet(RedisTestContext context) throws JsonProcessingException {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		String result = sync.jsonGet("obj");
		assertJSONEquals(JSON, result);
	}

	private void assertJSONEquals(String expected, String actual) throws JsonMappingException, JsonProcessingException {
		assertEquals(MAPPER.readTree(expected), MAPPER.readTree(actual));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonGetPaths(RedisTestContext context) throws JsonProcessingException {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		String result = sync.jsonGet("obj", ".name", ".loggedOut");
		assertJSONEquals("{\".name\":\"Leonard Cohen\",\".loggedOut\": true}", result);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonGetJSONPath(RedisTestContext context) throws JsonProcessingException {
		Assumptions.assumeFalse(context.getServer() instanceof RedisEnterpriseContainer);
		String json = "{\"a\":2, \"b\": 3, \"nested\": {\"a\": 4, \"b\": null}}";
		RedisModulesCommands<String, String> sync = context.sync();
		sync.jsonSet("doc", "$", json);
		assertEquals("[3,null]", sync.jsonGet("doc", "$..b"));
		assertJSONEquals("{\"$..b\":[3,null],\"..a\":[2,4]}", sync.jsonGet("doc", "..a", "$..b"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonGetOptions(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		String result = sync.jsonGet("obj",
				GetOptions.builder().indent("___").newline("#").noEscape(true).space("_").build());
		assertEquals("{#___\"name\":_\"Leonard Cohen\",#___\"lastSeen\":_1478476800,#___\"loggedOut\":_true#}", result);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonGetOptionsPaths(RedisTestContext context) throws JsonMappingException, JsonProcessingException {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		String result = sync.jsonGet("obj",
				GetOptions.builder().indent("  ").newline("\n").noEscape(true).space("   ").build(), ".name",
				".loggedOut");
		JsonNode resultNode = MAPPER.readTree(result);
		assertEquals("Leonard Cohen", resultNode.get(".name").asText());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonMget(RedisTestContext context) throws JsonProcessingException {
		RedisJSONCommands<String, String> sync = context.sync();
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

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonDel(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		sync.jsonDel("obj");
		String result = sync.jsonGet("obj");
		Assertions.assertNull(result);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonType(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		assertEquals("object", sync.jsonType("obj"));
		assertEquals("string", sync.jsonType("obj", ".name"));
		assertEquals("boolean", sync.jsonType("obj", ".loggedOut"));
		assertEquals("integer", sync.jsonType("obj", ".lastSeen"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonNumIncrBy(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		long lastSeen = 1478476800;
		double increment = 123.456;
		String result = sync.numincrby("obj", ".lastSeen", increment);
		assertEquals(lastSeen + increment, Double.parseDouble(result));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonNumMultBy(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		long lastSeen = 1478476800;
		double factor = 123.456;
		String result = sync.nummultby("obj", ".lastSeen", factor);
		assertEquals(lastSeen * factor, Double.parseDouble(result));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonStrings(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("foo", ".", "\"bar\"");
		assertEquals(3, sync.strlen("foo", "."));
		assertEquals("barbaz".length(), sync.strappend("foo", ".", "\"baz\""));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonArrays(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("arr", ".", "[]");
		assertEquals(1, sync.arrappend("arr", ".", "0"));
		assertEquals("[0]", sync.jsonGet("arr"));
		assertEquals(3, sync.arrinsert("arr", ".", 0, "-2", "-1"));
		assertEquals("[-2,-1,0]", sync.jsonGet("arr"));
		assertEquals(1, sync.arrtrim("arr", ".", 1, 1));
		assertEquals("[-1]", sync.jsonGet("arr"));
		assertEquals("-1", sync.arrpop("arr"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void jsonObjects(RedisTestContext context) {
		RedisJSONCommands<String, String> sync = context.sync();
		sync.jsonSet("obj", ".", JSON);
		assertEquals(3, sync.objlen("obj", "."));
		assertEquals(Arrays.asList("name", "lastSeen", "loggedOut"), sync.objkeys("obj", "."));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void rgPyExecute(RedisTestContext context) {
		assumeGears();
		RedisModulesCommands<String, String> sync = context.sync();
		sync.set("foo", "bar");
		ExecutionResults results = pyExecute(sync, "sleep.py");
		assertEquals("1", results.getResults().get(0));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void rgPyExecuteUnblocking(RedisTestContext context) {
		assumeGears();
		RedisModulesCommands<String, String> sync = context.sync();
		sync.set("foo", "bar");
		String executionId = pyExecuteUnblocking(sync, "sleep.py");
		String[] array = executionId.split("-");
		assertEquals(2, array.length);
		assertEquals(40, array[0].length());
		Assertions.assertTrue(Integer.parseInt(array[1]) >= 0);
	}

//    @ParameterizedTest
//    @RedisTestContextsSource
//    void pyExecuteNoResults(RedisTestContext context) {
//        RedisModulesCommands<String, String> sync = context.sync();
//        ExecutionResults results = pyExecute(sync, "sleep.py");
//        Assertions.assertTrue(results.getResults().isEmpty());
//        Assertions.assertTrue(results.getErrors().isEmpty());
//    }

	private ExecutionResults pyExecute(RedisGearsCommands<String, String> sync, String resourceName) {
		return sync.pyexecute(load(resourceName));
	}

	private String pyExecuteUnblocking(RedisGearsCommands<String, String> sync, String resourceName) {
		return sync.pyexecuteUnblocking(load(resourceName));
	}

	private String load(String resourceName) {
		return RedisModulesUtils.toString(getClass().getClassLoader().getResourceAsStream(resourceName));
	}

	private void clearGears(RedisTestContext context) throws InterruptedException {
		Thread.sleep(100);
		RedisModulesCommands<String, String> sync = context.sync();
		// Unregister all registrations
		for (Registration registration : sync.dumpregistrations()) {
			log.info("Unregistering {}", registration.getId());
			sync.unregister(registration.getId());
		}
		// Drop all executions
		for (Execution execution : sync.dumpexecutions()) {
			if (execution.getStatus().matches("running|created")) {
				log.info("Aborting execution {} with status {}", execution.getId(), execution.getStatus());
				sync.abortexecution(execution.getId());
			}
			try {
				sync.dropexecution(execution.getId());
			} catch (RedisCommandExecutionException e) {
				log.info("Execution status: {}", execution.getStatus());
				throw e;
			}
		}
	}

//	@ParameterizedTest
//	@RedisTestContextsSource
	void rgDumpRegistrations(RedisTestContext context) throws InterruptedException {
		assumeGears();
		clearGears(context);
		RedisModulesCommands<String, String> sync = context.sync();
		// Single registration
		List<Registration> registrations = sync.dumpregistrations();
		assertEquals(0, registrations.size());
		ExecutionResults results = pyExecute(sync, "streamreader.py");
		Assertions.assertFalse(results.isError());
		registrations = sync.dumpregistrations();
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
//		Assertions.assertTrue(registration.getPrivateData().contains("'sessionId'"));

		// Multiple registrations
		sync.dumpregistrations().forEach(r -> sync.unregister(r.getId()));
		String function = "GB('KeysReader').register('*', keyTypes=['hash'])";
		Assertions.assertTrue(sync.pyexecute(function).isOk());
//		Assertions.assertTrue(sync.pyexecute(function).isOk());
		Assertions.assertTrue(sync.dumpregistrations().size() == 1);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void rgPyExecuteResults(RedisTestContext context) {
		assumeGears();
		RedisModulesCommands<String, String> sync = context.sync();
		sync.set("foo", "bar");
		ExecutionResults results = sync.pyexecute("GB().foreach(lambda x: log('test')).register()");
		Assertions.assertTrue(results.isOk());
		Assertions.assertFalse(results.isError());
	}

	private void rgExecutions(RedisTestContext context) {
		RedisModulesCommands<String, String> sync = context.sync();
		sync.set("foo", "bar");
		pyExecuteUnblocking(sync, "sleep.py");
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void rgDumpExecutions(RedisTestContext context) throws InterruptedException {
		assumeGears();
		clearGears(context);
		rgExecutions(context);
		assertEquals(1, context.sync().dumpexecutions().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void rgDropExecution(RedisTestContext context) throws InterruptedException {
		assumeGears();
		clearGears(context);
		rgExecutions(context);
		RedisModulesCommands<String, String> sync = context.sync();
		List<Execution> executions = sync.dumpexecutions();
		executions.forEach(e -> sync.abortexecution(e.getId()));
		executions.forEach(e -> sync.dropexecution(e.getId()));
		assertEquals(0, sync.dumpexecutions().size());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void rgAbortExecution(RedisTestContext context) throws InterruptedException {
		assumeGears();
		clearGears(context);
		rgExecutions(context);
		RedisModulesCommands<String, String> sync = context.sync();
		for (Execution execution : sync.dumpexecutions()) {
			sync.abortexecution(execution.getId());
			ExecutionDetails details = sync.getexecution(execution.getId());
			Assertions.assertTrue(details.getPlan().getStatus().matches("done|aborted"));
		}
	}

	private void assumeGears() {
		Assumptions.assumeTrue(RedisServer.isEnabled("REDISGEARS"));
	}

	public static void main(String[] args) {
		System.out.println(System.getProperty("TESTCONTAINERS_REDIS_CLUSTER"));
	}
}
