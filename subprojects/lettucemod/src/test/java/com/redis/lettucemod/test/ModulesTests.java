package com.redis.lettucemod.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
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
import com.redis.lettucemod.util.RedisClientBuilder;
import com.redis.lettucemod.util.RedisClientOptions;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.AbstractTestcontainersRedisTestBase;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;
import com.redis.testcontainers.junit.RedisTestInstance;

import io.lettuce.core.AclSetuserArgs;
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
	private final RedisModulesContainer redisModulesContainer = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));
	private final RedisEnterpriseContainer redisEnterpriseContainer = new RedisEnterpriseContainer(
			RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag("latest"))
			.withDatabase(Database.name("ModulesTests").memory(DataSize.ofMegabytes(110)).ossCluster(true)
					.modules(RedisModule.SEARCH, RedisModule.JSON, RedisModule.GEARS, RedisModule.TIMESERIES).build());

	@SuppressWarnings("resource")
	@Override
	protected Collection<RedisServer> redisServers() {
		return Arrays.asList(redisModulesContainer, redisEnterpriseContainer);
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
		StatefulRedisModulesConnection<String, String> connection = context.getConnection();
		connection.setAutoFlushCommands(false);
		RedisModulesAsyncCommands<String, String> async = connection.async();
		List<RedisFuture<?>> futures = new ArrayList<>();
		while (beers.hasNext()) {
			Map<String, Object> beer = beers.next();
			futures.add(async.ftSugadd(SUGINDEX,
					Suggestion.string((String) beer.get(Beers.FIELD_NAME.getName())).score(1).build()));
		}
		connection.flushCommands();
		connection.setAutoFlushCommands(true);
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

	@TestInstance(Lifecycle.PER_CLASS)
	class NestedTestInstance implements RedisTestInstance {

		@Override
		public List<RedisTestContext> getContexts() {
			return ModulesTests.this.getContexts();
		}
	}

	@Nested
	class Search extends NestedTestInstance {

		@ParameterizedTest
		@RedisTestContextsSource
		void sugaddIncr(RedisTestContext context) {
			RedisModulesCommands<String, String> sync = context.sync();
			String key = "testSugadd";
			sync.ftSugadd(key, Suggestion.of("value1", 1));
			sync.ftSugaddIncr(key, Suggestion.of("value1", 1));
			List<Suggestion<String>> suggestions = sync.ftSugget(key, "value",
					SuggetOptions.builder().withScores(true).build());
			assertEquals(1, suggestions.size());
			assertEquals(1.4142135381698608, suggestions.get(0).getScore());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void sugaddPayload(RedisTestContext context) {
			RedisModulesCommands<String, String> sync = context.sync();
			String key = "testSugadd";
			sync.ftSugadd(key, Suggestion.string("value1").score(1).payload("somepayload").build());
			List<Suggestion<String>> suggestions = sync.ftSugget(key, "value",
					SuggetOptions.builder().withPayloads(true).build());
			assertEquals(1, suggestions.size());
			assertEquals("somepayload", suggestions.get(0).getPayload());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void sugaddScorePayload(RedisTestContext context) {
			RedisModulesCommands<String, String> sync = context.sync();
			String key = "testSugadd";
			sync.ftSugadd(key, Suggestion.string("value1").score(2).payload("somepayload").build());
			List<Suggestion<String>> suggestions = sync.ftSugget(key, "value",
					SuggetOptions.builder().withScores(true).withPayloads(true).build());
			assertEquals(1, suggestions.size());
			assertEquals(1.4142135381698608, suggestions.get(0).getScore());
			assertEquals("somepayload", suggestions.get(0).getPayload());
		}

		@SuppressWarnings("unchecked")
		@ParameterizedTest
		@RedisTestContextsSource
		void create(RedisTestContext context) throws Exception {
			int count = Beers.populateIndex(context.getConnection());
			RedisModulesCommands<String, String> sync = context.sync();
			assertEquals(count, RedisModulesUtils.indexInfo(sync.ftInfo(Beers.INDEX)).getNumDocs());
			CreateOptions<String, String> options = CreateOptions.<String, String>builder().prefix("release:")
					.payloadField("xml").build();
			Field<String>[] fields = new Field[] { Field.text("artist").sortable().build(),
					Field.tag("id").sortable().build(), Field.text("title").sortable().build() };
			sync.ftCreate("releases", options, fields);
			assertEquals(fields.length, RedisModulesUtils.indexInfo(sync.ftInfo("releases")).getFields().size());
		}

		@SuppressWarnings("unchecked")
		@ParameterizedTest
		@RedisTestContextsSource
		void createTemporaryIndex(RedisTestContext context) throws Exception {
			Beers.populateIndex(context.getConnection());
			RedisModulesCommands<String, String> sync = context.sync();
			String tempIndex = "temporaryIndex";
			sync.ftCreate(tempIndex, CreateOptions.<String, String>builder().temporary(1L).build(),
					Field.text("field1").build());
			assertEquals(tempIndex, RedisModulesUtils.indexInfo(sync.ftInfo(tempIndex)).getIndexName());
			Awaitility.await().until(() -> !sync.ftList().contains(tempIndex));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void alterIndex(RedisTestContext context) throws Exception {
			Beers.populateIndex(context.getConnection());
			RedisModulesCommands<String, String> sync = context.sync();
			Map<String, Object> indexInfo = toMap(sync.ftInfo(Beers.INDEX));
			assertEquals(Beers.INDEX, indexInfo.get("index_name"));
			sync.ftAlter(Beers.INDEX, Field.tag("newField").build());
			Map<String, String> doc = mapOf("newField", "value1");
			sync.hmset("beer:newDoc", doc);
			SearchResults<String, String> results = sync.ftSearch(Beers.INDEX, "@newField:{value1}");
			assertEquals(1, results.getCount());
			assertEquals(doc.get("newField"), results.get(0).get("newField"));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void dropindexDeleteDocs(RedisTestContext context) throws Exception {
			Beers.populateIndex(context.getConnection());
			RedisModulesCommands<String, String> sync = context.sync();
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
		@ParameterizedTest
		@RedisTestContextsSource
		void list(RedisTestContext context) throws ExecutionException, InterruptedException {
			RedisModulesCommands<String, String> sync = context.sync();
			sync.flushall();
			Set<String> indexNames = new HashSet<>(Arrays.asList("index1", "index2", "index3"));
			for (String indexName : indexNames) {
				sync.ftCreate(indexName, Field.text("field1").sortable().build());
			}
			assertEquals(indexNames, new HashSet<>(sync.ftList()));
			assertEquals(indexNames, new HashSet<>(context.async().ftList().get()));
			assertEquals(indexNames, new HashSet<>(context.reactive().ftList().collectList().block()));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void searchOptions(RedisTestContext context) throws IOException {
			Beers.populateIndex(context.getConnection());
			RedisModulesCommands<String, String> sync = context.sync();
			SearchOptions<String, String> options = SearchOptions.<String, String>builder().withPayloads(true)
					.noStopWords(true).limit(SearchOptions.limit(10, 100)).withScores(true)
					.highlight(Highlight.<String, String>builder().field(Beers.FIELD_NAME.getName())
							.tags("<TAG>", "</TAG>").build())
					.language(Language.ENGLISH).noContent(false)
					.sortBy(SearchOptions.SortBy.asc(Beers.FIELD_NAME.getName())).verbatim(false).withSortKeys(true)
					.returnField(Beers.FIELD_NAME.getName()).returnField(Beers.FIELD_STYLE_NAME.getName()).build();
			SearchResults<String, String> results = sync.ftSearch(Beers.INDEX, "pale", options);
			assertEquals(74, results.getCount());
			Document<String, String> doc1 = results.get(0);
			assertNotNull(doc1.get(Beers.FIELD_NAME.getName()));
			assertNotNull(doc1.get(Beers.FIELD_STYLE_NAME.getName()));
			assertNull(doc1.get(Beers.FIELD_ABV.getName()));

		}

		private void assertSearch(RedisTestContext context, String query, SearchOptions<String, String> options,
				long expectedCount, String... expectedAbv) {
			SearchResults<String, String> results = context.sync().ftSearch(Beers.INDEX, query, options);
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
		void search(RedisTestContext context) throws Exception {
			Beers.populateIndex(context.getConnection());
			RedisModulesCommands<String, String> sync = context.sync();
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

			assertSearch(context, "pale", SearchOptions.<String, String>builder()
					.returnField(Beers.FIELD_NAME.getName()).returnField(Beers.FIELD_STYLE_NAME.getName()).build(), 74);
			assertSearch(context, "pale",
					SearchOptions.<String, String>builder().returnField(Beers.FIELD_NAME.getName())
							.returnField(Beers.FIELD_STYLE_NAME.getName()).returnField("").build(),
					74);
			assertSearch(context, "*", SearchOptions.<String, String>builder().inKeys("beer:728", "beer:803").build(),
					2, "5.800000190734863", "8");
			assertSearch(context, "wise",
					SearchOptions.<String, String>builder().inField(Beers.FIELD_NAME.getName()).build(), 1,
					"5.900000095367432");
		}

		@SuppressWarnings("unchecked")
		@ParameterizedTest
		@RedisTestContextsSource
		void searchTags(RedisTestContext context) throws InterruptedException, ExecutionException, IOException {
			int count = Beers.populateIndex(context.getConnection());
			RedisModulesCommands<String, String> sync = context.sync();
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

			results = context.reactive().ftSearch(Beers.INDEX, "pale",
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
			context.async().ftCreate(index, Field.tag(idField).build()).get();
			Map<String, String> doc1 = new HashMap<>();
			doc1.put(idField, "chris@blah.org,User1#test.org,usersdfl@example.com");
			context.async().hmset("doc1", doc1).get();
			results = context.async().ftSearch(index, "@id:{" + RedisModulesUtils.escapeTag("User1#test.org") + "}")
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

		@ParameterizedTest
		@RedisTestContextsSource
		void sugget(RedisTestContext context) throws IOException {
			createBeerSuggestions(context);
			RedisModulesCommands<String, String> sync = context.sync();
			RedisModulesReactiveCommands<String, String> reactive = context.reactive();
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
			loadAsserts.accept(sync.ftAggregate(Beers.INDEX, "*", loadOptions));
			loadAsserts.accept(reactive.ftAggregate(Beers.INDEX, "*", loadOptions).block());

			// GroupBy tests
			Consumer<AggregateResults<String>> groupByAsserts = results -> {
				assertEquals(36, results.getCount());
				List<Double> abvs = results.stream()
						.map(r -> Double.parseDouble((String) r.get(Beers.FIELD_ABV.getName())))
						.collect(Collectors.toList());
				assertTrue(abvs.get(0) > abvs.get(abvs.size() - 1));
				assertEquals(20, results.size());
			};
			AggregateOptions<String, String> groupByOptions = AggregateOptions
					.<String, String>operation(Group.by(Beers.FIELD_STYLE_NAME.getName())
							.reducer(Avg.property(Beers.FIELD_ABV.getName()).as(Beers.FIELD_ABV.getName()).build())
							.build())
					.operation(Sort.by(Sort.Property.desc(Beers.FIELD_ABV.getName())).build())
					.operation(Limit.offset(0).num(20)).build();
			groupByAsserts.accept(sync.ftAggregate(Beers.INDEX, "*", groupByOptions));
			groupByAsserts.accept(reactive.ftAggregate(Beers.INDEX, "*", groupByOptions).block());

			Consumer<AggregateResults<String>> groupBy0Asserts = results -> {
				assertEquals(1, results.getCount());
				assertEquals(1, results.size());
				Double maxAbv = Double.parseDouble((String) results.get(0).get(Beers.FIELD_ABV.getName()));
				assertEquals(16, maxAbv, 0.1);
			};

			AggregateOptions<String, String> groupBy0Options = AggregateOptions.<String, String>operation(Group.by()
					.reducer(Max.property(Beers.FIELD_ABV.getName()).as(Beers.FIELD_ABV.getName()).build()).build())
					.build();
			groupBy0Asserts.accept(sync.ftAggregate(Beers.INDEX, "*", groupBy0Options));
			groupBy0Asserts.accept(reactive.ftAggregate(Beers.INDEX, "*", groupBy0Options).block());

			Consumer<AggregateResults<String>> groupBy2Asserts = results -> {
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
			AggregateOptions<String, String> groupBy2Options = AggregateOptions.<String, String>operation(group)
					.operation(Limit.offset(0).num(2)).build();
			groupBy2Asserts.accept(sync.ftAggregate(Beers.INDEX, "*", groupBy2Options));
			groupBy2Asserts.accept(reactive.ftAggregate(Beers.INDEX, "*", groupBy2Options).block());

			// Cursor tests
			Consumer<AggregateWithCursorResults<String>> cursorTests = cursorResults -> {
				assertEquals(1, cursorResults.getCount());
				assertEquals(10, cursorResults.size());
				assertTrue(((String) cursorResults.get(9).get(Beers.FIELD_ABV.getName())).length() > 0);
			};
			AggregateOptions<String, String> cursorOptions = AggregateOptions.<String, String>builder()
					.load(Beers.FIELD_ID.getName()).load(Beers.FIELD_NAME.getName()).load(Beers.FIELD_ABV.getName())
					.build();
			AggregateWithCursorResults<String> cursorResults = sync.ftAggregate(Beers.INDEX, "*",
					CursorOptions.builder().count(10).build(), cursorOptions);
			cursorTests.accept(cursorResults);
			cursorTests.accept(reactive
					.ftAggregate(Beers.INDEX, "*", CursorOptions.builder().count(10).build(), cursorOptions).block());
			cursorResults = sync.ftCursorRead(Beers.INDEX, cursorResults.getCursor(), 400);
			assertEquals(400, cursorResults.size());
			String deleteStatus = sync.ftCursorDelete(Beers.INDEX, cursorResults.getCursor());
			assertEquals("OK", deleteStatus);
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void alias(RedisTestContext context) throws Exception {

			Beers.populateIndex(context.getConnection());
			// SYNC

			String alias = "alias123";

			RedisModulesCommands<String, String> sync = context.sync();
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
			RedisModulesAsyncCommands<String, String> async = context.async();
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

			RedisModulesReactiveCommands<String, String> reactive = context.reactive();
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

		@ParameterizedTest
		@RedisTestContextsSource
		void info(RedisTestContext context) throws Exception {
			int count = Beers.populateIndex(context.getConnection());
			IndexInfo info = RedisModulesUtils.indexInfo(context.async().ftInfo(Beers.INDEX).get());
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
		@ParameterizedTest
		@RedisTestContextsSource
		void jsonSearch(RedisTestContext context) throws Exception {
			Iterator<JsonNode> iterator = Beers.jsonNodeIterator();
			String index = "beers";
			TagField<String> idField = Field.tag(jsonField(Beers.FIELD_ID.getName())).as(Beers.FIELD_ID.getName())
					.build();
			TextField<String> nameField = Field.text(jsonField(Beers.FIELD_NAME.getName()))
					.as(Beers.FIELD_NAME.getName()).build();
			TextField<String> styleField = Field.text(jsonField(Beers.FIELD_STYLE_NAME.getName())).build();
			context.sync().ftCreate(index, CreateOptions.<String, String>builder().on(DataType.JSON).build(), idField,
					nameField, styleField);
			IndexInfo info = RedisModulesUtils.indexInfo(context.sync().ftInfo(index));
			Assertions.assertEquals(3, info.getFields().size());
			Assertions.assertEquals(idField.getAs(), info.getFields().get(0).getAs());
			Assertions.assertEquals(styleField.getName(), info.getFields().get(2).getAs().get());
			while (iterator.hasNext()) {
				JsonNode beer = iterator.next();
				context.sync().jsonSet("beer:" + beer.get(Beers.FIELD_ID.getName()).asText(), "$", beer.toString());
			}
			SearchResults<String, String> results = context.sync().ftSearch(index,
					"@" + Beers.FIELD_NAME.getName() + ":Creek");
			Assertions.assertEquals(1, results.getCount());
		}

		private String jsonField(String name) {
			return "$." + name;
		}

		@SuppressWarnings("unchecked")
		@ParameterizedTest
		@RedisTestContextsSource
		void infoFields(RedisTestContext context) {
			String index = "indexFields";
			TagField<String> idField = Field.tag(jsonField(Beers.FIELD_ID.getName())).as(Beers.FIELD_ID.getName())
					.separator('-').build();
			TextField<String> nameField = Field.text(jsonField(Beers.FIELD_NAME.getName()))
					.as(Beers.FIELD_NAME.getName()).noIndex().noStem().sortable().weight(2).build();
			String styleFieldName = jsonField(Beers.FIELD_STYLE_NAME.getName());
			TextField<String> styleField = Field.text(styleFieldName).as(styleFieldName).weight(1).build();
			context.sync().ftCreate(index, CreateOptions.<String, String>builder().on(DataType.JSON).build(), idField,
					nameField, styleField);
			IndexInfo info = RedisModulesUtils.indexInfo(context.sync().ftInfo(index));
			Assertions.assertEquals(idField, info.getFields().get(0));
			Assertions.assertEquals(nameField, info.getFields().get(1));
			Assertions.assertEquals(styleField, info.getFields().get(2));
		}

		@SuppressWarnings("unchecked")
		@ParameterizedTest
		@RedisTestContextsSource
		void infoOptions(RedisTestContext context) {
			RedisModulesCommands<String, String> commands = context.sync();
			String index = "indexWithOptions";
			CreateOptions<String, String> createOptions = CreateOptions.<String, String>builder().on(DataType.JSON)
					.prefixes("prefix1", "prefix2").filter("@indexName==\"myindexname\"")
					.defaultLanguage(Language.CHINESE).languageField("languageField").defaultScore(.5)
					.scoreField("scoreField").payloadField("payloadField").maxTextFields(true).noOffsets(true)
					.noFields(true).noFreqs(true).build();
			commands.ftCreate(index, createOptions, Field.tag("id").build(), Field.numeric("scoreField").build());
			IndexInfo info = RedisModulesUtils.indexInfo(commands.ftInfo(index));
			CreateOptions<String, String> actual = info.getIndexOptions();
			Assertions.assertEquals(createOptions, actual);
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void tagVals(RedisTestContext context) throws Exception {
			Beers.populateIndex(context.getConnection());
			Set<String> TAG_VALS = new HashSet<>(Arrays.asList(
					"american-style brown ale, traditional german-style bock, german-style schwarzbier, old ale, american-style india pale ale, german-style oktoberfest, other belgian-style ales, american-style stout, winter warmer, belgian-style tripel, american-style lager, belgian-style dubbel, porter, american-style barley wine ale, belgian-style fruit lambic, scottish-style light ale, south german-style hefeweizen, imperial or double india pale ale, golden or blonde ale, belgian-style quadrupel, american-style imperial stout, belgian-style pale strong ale, english-style pale mild ale, american-style pale ale, irish-style red ale, dark american-belgo-style ale, light american wheat ale or lager, german-style pilsener, american-style amber/red ale, scotch ale, german-style doppelbock, extra special bitter, south german-style weizenbock, english-style india pale ale, belgian-style pale ale, french & belgian-style saison"
							.split(", ")));
			HashSet<String> actual = new HashSet<>(
					context.sync().ftTagvals(Beers.INDEX, Beers.FIELD_STYLE_NAME.getName()));
			assertEquals(TAG_VALS, actual);
			assertEquals(TAG_VALS, new HashSet<>(
					context.reactive().ftTagvals(Beers.INDEX, Beers.FIELD_STYLE_NAME.getName()).collectList().block()));
		}

		@SuppressWarnings("unchecked")
		@ParameterizedTest
		@RedisTestContextsSource
		void emptyToListReducer(RedisTestContext context) {
			RedisModulesCommands<String, String> sync = context.sync();
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

		@ParameterizedTest
		@RedisTestContextsSource
		void dictadd(RedisTestContext context) {
			String[] DICT_TERMS = new String[] { "beer", "ale", "brew", "brewski" };
			RedisModulesCommands<String, String> sync = context.sync();
			assertEquals(DICT_TERMS.length, sync.ftDictadd("beers", DICT_TERMS));
			assertEquals(new HashSet<>(Arrays.asList(DICT_TERMS)), new HashSet<>(sync.ftDictdump("beers")));
			assertEquals(1, sync.ftDictdel("beers", "brew"));
			List<String> beerDict = new ArrayList<>();
			Collections.addAll(beerDict, DICT_TERMS);
			beerDict.remove("brew");
			assertEquals(new HashSet<>(beerDict), new HashSet<>(sync.ftDictdump("beers")));
			context.sync().flushall();
			RedisModulesReactiveCommands<String, String> reactive = context.reactive();
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
	class TimeSeries extends NestedTestInstance {

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
		@ParameterizedTest
		@RedisTestContextsSource
		void create(RedisTestContext context) {
			String status = context.sync().tsCreate(KEY, com.redis.lettucemod.timeseries.CreateOptions
					.<String, String>builder().retentionPeriod(6000).build());
			assertEquals("OK", status);
			assertEquals("OK",
					context.sync().tsCreate("virag",
							com.redis.lettucemod.timeseries.CreateOptions.<String, String>builder()
									.retentionPeriod(100000L).labels(Label.of("name", "value"))
									.policy(DuplicatePolicy.LAST).build()));
		}

		@SuppressWarnings("unchecked")
		@ParameterizedTest
		@RedisTestContextsSource
		void add(RedisTestContext context) {
			RedisTimeSeriesCommands<String, String> ts = context.sync();
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

		@ParameterizedTest
		@RedisTestContextsSource
		void range(RedisTestContext context) {
			RedisTimeSeriesCommands<String, String> ts = context.sync();
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

		@ParameterizedTest
		@RedisTestContextsSource
		void mget(RedisTestContext context) {
			RedisTimeSeriesCommands<String, String> ts = context.sync();
			populate(ts);
			List<GetResult<String, String>> results = ts.tsMget(FILTER);
			Assertions.assertEquals(2, results.size());
			Assertions.assertEquals(TIMESTAMP_2, results.get(0).getSample().getTimestamp());
			Assertions.assertEquals(VALUE_2, results.get(0).getSample().getValue());
			Assertions.assertEquals(TIMESTAMP_2, results.get(1).getSample().getTimestamp());
			Assertions.assertEquals(VALUE_2, results.get(1).getSample().getValue());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void get(RedisTestContext context) {
			RedisTimeSeriesCommands<String, String> ts = context.sync();
			populate(ts);
			Sample result = ts.tsGet(KEY);
			Assertions.assertEquals(TIMESTAMP_2, result.getTimestamp());
			Assertions.assertEquals(VALUE_2, result.getValue());
			ts.tsCreate("ts:empty", com.redis.lettucemod.timeseries.CreateOptions.<String, String>builder().build());
			Assertions.assertNull(ts.tsGet("ts:empty"));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void mrange(RedisTestContext context) {
			RedisTimeSeriesCommands<String, String> ts = context.sync();
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
	class Json extends NestedTestInstance {

		private static final String JSON = "{\"name\":\"Leonard Cohen\",\"lastSeen\":1478476800,\"loggedOut\": true}";

		@ParameterizedTest
		@RedisTestContextsSource
		void set(RedisTestContext context) {
			RedisJSONCommands<String, String> sync = context.sync();
			String result = sync.jsonSet("obj", ".", JSON);
			assertEquals("OK", result);
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void setNX(RedisTestContext context) {
			RedisJSONCommands<String, String> sync = context.sync();
			sync.jsonSet("obj", ".", JSON);
			String result = sync.jsonSet("obj", ".", JSON, SetMode.NX);
			Assertions.assertNull(result);
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void setXX(RedisTestContext context) {
			RedisJSONCommands<String, String> sync = context.sync();
			String result = sync.jsonSet("obj", ".", "true", SetMode.XX);
			Assertions.assertNull(result);
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void get(RedisTestContext context) throws JsonProcessingException {
			RedisJSONCommands<String, String> sync = context.sync();
			sync.jsonSet("obj", ".", JSON);
			String result = sync.jsonGet("obj");
			assertJSONEquals(JSON, result);
		}

		private void assertJSONEquals(String expected, String actual)
				throws JsonMappingException, JsonProcessingException {
			ObjectMapper mapper = new ObjectMapper();
			assertEquals(mapper.readTree(expected), mapper.readTree(actual));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void getPaths(RedisTestContext context) throws JsonProcessingException {
			RedisJSONCommands<String, String> sync = context.sync();
			sync.jsonSet("obj", ".", JSON);
			String result = sync.jsonGet("obj", ".name", ".loggedOut");
			assertJSONEquals("{\".name\":\"Leonard Cohen\",\".loggedOut\": true}", result);
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void getPath(RedisTestContext context) throws JsonProcessingException {
			Assumptions.assumeFalse(context.getServer() instanceof RedisEnterpriseContainer);
			String json = "{\"a\":2, \"b\": 3, \"nested\": {\"a\": 4, \"b\": null}}";
			RedisModulesCommands<String, String> sync = context.sync();
			sync.jsonSet("doc", "$", json);
			assertEquals("[3,null]", sync.jsonGet("doc", "$..b"));
			assertJSONEquals("{\"$..b\":[3,null],\"..a\":[2,4]}", sync.jsonGet("doc", "..a", "$..b"));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void getOptions(RedisTestContext context) {
			RedisJSONCommands<String, String> sync = context.sync();
			sync.jsonSet("obj", ".", JSON);
			String result = sync.jsonGet("obj",
					GetOptions.builder().indent("___").newline("#").noEscape(true).space("_").build());
			assertEquals("{#___\"name\":_\"Leonard Cohen\",#___\"lastSeen\":_1478476800,#___\"loggedOut\":_true#}",
					result);
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void getOptionsPaths(RedisTestContext context) throws JsonMappingException, JsonProcessingException {
			RedisJSONCommands<String, String> sync = context.sync();
			sync.jsonSet("obj", ".", JSON);
			String result = sync.jsonGet("obj",
					GetOptions.builder().indent("  ").newline("\n").noEscape(true).space("   ").build(), ".name",
					".loggedOut");
			JsonNode resultNode = new ObjectMapper().readTree(result);
			assertEquals("Leonard Cohen", resultNode.get(".name").asText());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void mget(RedisTestContext context) throws JsonProcessingException {
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
		void del(RedisTestContext context) {
			RedisJSONCommands<String, String> sync = context.sync();
			sync.jsonSet("obj", ".", JSON);
			sync.jsonDel("obj");
			String result = sync.jsonGet("obj");
			Assertions.assertNull(result);
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void type(RedisTestContext context) {
			RedisJSONCommands<String, String> sync = context.sync();
			sync.jsonSet("obj", ".", JSON);
			assertEquals("object", sync.jsonType("obj"));
			assertEquals("string", sync.jsonType("obj", ".name"));
			assertEquals("boolean", sync.jsonType("obj", ".loggedOut"));
			assertEquals("integer", sync.jsonType("obj", ".lastSeen"));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void numIncrBy(RedisTestContext context) {
			RedisJSONCommands<String, String> sync = context.sync();
			sync.jsonSet("obj", ".", JSON);
			long lastSeen = 1478476800;
			double increment = 123.456;
			String result = sync.jsonNumincrby("obj", ".lastSeen", increment);
			assertEquals(lastSeen + increment, Double.parseDouble(result));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void numMultBy(RedisTestContext context) {
			RedisJSONCommands<String, String> sync = context.sync();
			sync.jsonSet("obj", ".", JSON);
			long lastSeen = 1478476800;
			double factor = 123.456;
			String result = sync.jsonNummultby("obj", ".lastSeen", factor);
			assertEquals(lastSeen * factor, Double.parseDouble(result));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void strings(RedisTestContext context) {
			RedisJSONCommands<String, String> sync = context.sync();
			sync.jsonSet("foo", ".", "\"bar\"");
			assertEquals(3, sync.jsonStrlen("foo", "."));
			assertEquals("barbaz".length(), sync.jsonStrappend("foo", ".", "\"baz\""));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void arrays(RedisTestContext context) {
			RedisJSONCommands<String, String> sync = context.sync();
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

		@ParameterizedTest
		@RedisTestContextsSource
		void objects(RedisTestContext context) {
			RedisJSONCommands<String, String> sync = context.sync();
			sync.jsonSet("obj", ".", JSON);
			assertEquals(3, sync.jsonObjlen("obj", "."));
			assertEquals(Arrays.asList("name", "lastSeen", "loggedOut"), sync.jsonObjkeys("obj", "."));
		}
	}

	@Nested
	class Gears extends NestedTestInstance {

		@ParameterizedTest
		@RedisTestContextsSource
		void pyExecute(RedisTestContext context) {
			assumeGears();
			RedisModulesCommands<String, String> sync = context.sync();
			sync.set("foo", "bar");
			ExecutionResults results = pyExecute(sync, "sleep.py");
			assertEquals("1", results.getResults().get(0));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void pyExecuteUnblocking(RedisTestContext context) {
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
			return sync.rgPyexecute(load(resourceName));
		}

		private String pyExecuteUnblocking(RedisGearsCommands<String, String> sync, String resourceName) {
			return sync.rgPyexecuteUnblocking(load(resourceName));
		}

		private String load(String resourceName) {
			return RedisModulesUtils.toString(getClass().getClassLoader().getResourceAsStream(resourceName));
		}

		private void clearGears(RedisTestContext context) throws InterruptedException {
			RedisModulesCommands<String, String> sync = context.sync();
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

//	@ParameterizedTest
//	@RedisTestContextsSource
		void dumpRegistrations(RedisTestContext context) throws InterruptedException {
			assumeGears();
			clearGears(context);
			RedisModulesCommands<String, String> sync = context.sync();
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
//		Assertions.assertTrue(registration.getPrivateData().contains("'sessionId'"));

			// Multiple registrations
			sync.rgDumpregistrations().forEach(r -> sync.rgUnregister(r.getId()));
			String function = "GB('KeysReader').register('*', keyTypes=['hash'])";
			Assertions.assertTrue(sync.rgPyexecute(function).isOk());
//		Assertions.assertTrue(sync.pyexecute(function).isOk());
			Assertions.assertEquals(1, sync.rgDumpregistrations().size());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void pyExecuteResults(RedisTestContext context) {
			assumeGears();
			RedisModulesCommands<String, String> sync = context.sync();
			sync.set("foo", "bar");
			ExecutionResults results = sync.rgPyexecute("GB().foreach(lambda x: log('test')).register()");
			Assertions.assertTrue(results.isOk());
			Assertions.assertFalse(results.isError());
		}

		private void executions(RedisTestContext context) {
			RedisModulesCommands<String, String> sync = context.sync();
			sync.set("foo", "bar");
			pyExecuteUnblocking(sync, "sleep.py");
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void dumpExecutions(RedisTestContext context) throws InterruptedException {
			assumeGears();
			clearGears(context);
			executions(context);
			assertFalse(context.sync().rgDumpexecutions().isEmpty());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void dropExecution(RedisTestContext context) throws InterruptedException {
			assumeGears();
			clearGears(context);
			executions(context);
			RedisModulesCommands<String, String> sync = context.sync();
			List<Execution> executions = sync.rgDumpexecutions();
			executions.forEach(e -> sync.rgAbortexecution(e.getId()));
			executions.forEach(e -> sync.rgDropexecution(e.getId()));
			assertEquals(0, sync.rgDumpexecutions().size());
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void abortExecution(RedisTestContext context) throws InterruptedException {
			assumeGears();
			clearGears(context);
			executions(context);
			RedisModulesCommands<String, String> sync = context.sync();
			for (Execution execution : sync.rgDumpexecutions()) {
				sync.rgAbortexecution(execution.getId());
				ExecutionDetails details = sync.rgGetexecution(execution.getId());
				Assertions.assertTrue(details.getPlan().getStatus().matches("done|aborted"));
			}
		}

		private void assumeGears() {
			Assumptions.assumeTrue(RedisServer.isEnabled("REDISGEARS"));
		}
	}

	@Nested
	class Utils extends NestedTestInstance {

		@Test
		void credentials() {
			RedisTestContext context = getContext(redisModulesContainer);
			String username = "alice";
			String password = "ecila";
			context.sync().aclSetuser(username,
					AclSetuserArgs.Builder.on().addPassword(password).allCommands().allKeys());
			try {
				RedisClientOptions options = RedisClientOptions.builder().uriString(context.getRedisURI())
						.cluster(context.isCluster()).username(username).password("wrongpassword").build();
				RedisModulesUtils.connection(RedisClientBuilder.create(options).client());
				Assertions.fail("Expected connection failure");
			} catch (Exception e) {
				// expected
			}
			String key = "foo";
			String value = "bar";
			RedisClientOptions options = RedisClientOptions.builder().uriString(context.getRedisURI())
					.cluster(context.isCluster()).username(username).password(password).build();
			StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils
					.connection(RedisClientBuilder.create(options).client());
			connection.sync().set(key, value);
			Assertions.assertEquals(value, connection.sync().get(key));
		}

		@ParameterizedTest
		@RedisTestContextsSource
		void hostAndPort(RedisTestContext context) {
			RedisURI redisURI = RedisURI.create(context.getRedisURI());
			RedisClientOptions options = RedisClientOptions.builder().host(redisURI.getHost()).port(redisURI.getPort())
					.cluster(context.isCluster()).build();
			StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils
					.connection(RedisClientBuilder.create(options).client());
			Assertions.assertEquals("PONG", connection.sync().ping());
		}
	}

}
