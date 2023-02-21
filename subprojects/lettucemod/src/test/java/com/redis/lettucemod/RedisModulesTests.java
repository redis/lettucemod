package com.redis.lettucemod;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;

class RedisModulesTests extends BaseModulesTests {

	private final RedisModulesContainer redisContainer = new RedisModulesContainer(
			RedisModulesContainer.DEFAULT_IMAGE_NAME.withTag(RedisModulesContainer.DEFAULT_TAG));

	@Override
	protected RedisServer getRedisServer() {
		return redisContainer;
	}

	@Test
	void getPath() throws JsonProcessingException {
		String json = "{\"a\":2, \"b\": 3, \"nested\": {\"a\": 4, \"b\": null}}";
		RedisModulesCommands<String, String> sync = connection.sync();
		sync.jsonSet("doc", "$", json);
		assertEquals("[3,null]", sync.jsonGet("doc", "$..b"));
		assertJSONEquals("{\"$..b\":[3,null],\"..a\":[2,4]}", sync.jsonGet("doc", "..a", "$..b"));
	}

}
