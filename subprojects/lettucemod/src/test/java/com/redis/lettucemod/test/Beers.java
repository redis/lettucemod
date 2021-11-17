package com.redis.lettucemod.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.Field;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;

public class Beers {

	private static final String FILE = "beers.json";
	public static final String PREFIX = "beer:";
	public static final String INDEX = "beers";

	public static final Field FIELD_ID = Field.tag("id").sortable().build();
	public static final Field FIELD_BREWERY_ID = Field.tag("brewery_id").sortable().build();
	public static final Field FIELD_NAME = Field.text("name").sortable().build();
	public static final Field FIELD_ABV = Field.numeric("abv").sortable().build();
	public static final Field FIELD_IBU = Field.numeric("ibu").sortable().build();
	public static final Field FIELD_DESCRIPTION = Field.text("descript").build();
	public static final Field FIELD_STYLE_NAME = Field.tag("style_name").sortable().build();
	public static final Field FIELD_CATEGORY_NAME = Field.tag("cat_name").sortable().build();
	public static final Field[] SCHEMA = new Field[] { FIELD_ID, FIELD_NAME, FIELD_STYLE_NAME, FIELD_CATEGORY_NAME,
			FIELD_BREWERY_ID, FIELD_DESCRIPTION, FIELD_ABV, FIELD_IBU };
	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static void createIndex(RedisModulesCommands<String, String> commands) {
		commands.create(INDEX, CreateOptions.<String, String>builder().prefix(PREFIX).payloadField(FIELD_DESCRIPTION.getName()).build(), SCHEMA);
	}

	public static Iterator<JsonNode> jsonNodeIterator() throws IOException {
		return MAPPER.readerFor(Map.class).readTree(inputStream()).iterator();
	}

	public static MappingIterator<Map<String, Object>> mapIterator() throws IOException {
		return MAPPER.readerFor(Map.class).readValues(inputStream());
	}

	private static InputStream inputStream() {
		return Beers.class.getClassLoader().getResourceAsStream(FILE);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static int populateIndex(StatefulRedisModulesConnection<String, String> connection) throws IOException {
		createIndex(connection.sync());
		RedisModulesAsyncCommands<String, String> async = connection.async();
		async.setAutoFlushCommands(false);
		List<RedisFuture<?>> futures = new ArrayList<>();
		try {
			MappingIterator<Map<String, Object>> iterator = mapIterator();
			while (iterator.hasNext()) {
				Map<String, Object> beer = iterator.next();
				futures.add(async.hset(PREFIX + beer.get(FIELD_ID.getName()), (Map) beer));
			}
			async.flushCommands();
			LettuceFutures.awaitAll(connection.getTimeout(), futures.toArray(new RedisFuture[0]));
		} finally {
			async.setAutoFlushCommands(true);
		}
		return futures.size();
	}
}