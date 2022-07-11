package com.redis.lettucemod.test;

import java.util.Iterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.fasterxml.jackson.databind.JsonNode;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RediSearchCommands;
import com.redis.lettucemod.api.sync.RedisJSONCommands;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.CreateOptions.DataType;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchResults;
import com.redis.testcontainers.RedisModulesContainer;

@Testcontainers
class JsonSearchTests {

	@Container
	protected static final RedisModulesContainer REDISMOD_PREVIEW = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));

	@SuppressWarnings("unchecked")
	@Test
	void jsonSearch() throws Exception {
		Iterator<JsonNode> iterator = Beers.jsonNodeIterator();
		RedisModulesClient client = RedisModulesClient.create(REDISMOD_PREVIEW.getRedisURI());
		StatefulRedisModulesConnection<String, String> connection = client.connect();
		RediSearchCommands<String, String> redisearch = connection.sync();
		String index = "beers";
		redisearch.ftCreate(index, CreateOptions.<String, String>builder().on(DataType.JSON).build(),
				Field.tag("$." + Beers.FIELD_ID.getName()).as(Beers.FIELD_ID.getName()).build(),
				Field.text("$." + Beers.FIELD_NAME.getName()).as(Beers.FIELD_NAME.getName()).build(),
				Field.text("$." + Beers.FIELD_STYLE_NAME.getName()).as(Beers.FIELD_STYLE_NAME.getName()).build());
		RedisJSONCommands<String, String> redisjson = connection.sync();
		while (iterator.hasNext()) {
			JsonNode beer = iterator.next();
			redisjson.jsonSet("beer:" + beer.get(Beers.FIELD_ID.getName()).asText(), "$", beer.toString());
		}
		SearchResults<String, String> results = redisearch.ftSearch(index,
				"@" + Beers.FIELD_NAME.getName() + ":California");
		Assertions.assertEquals(5, results.getCount());
		connection.close();
		client.shutdown();
		client.getResources().shutdown();
	}

}
