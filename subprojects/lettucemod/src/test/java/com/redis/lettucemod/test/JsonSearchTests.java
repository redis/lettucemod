package com.redis.lettucemod.test;

import java.util.Iterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.fasterxml.jackson.databind.JsonNode;
import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.search.CreateOptions;
import com.redis.lettucemod.search.CreateOptions.DataType;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.IndexInfo;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.TagField;
import com.redis.lettucemod.search.TextField;
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
		String index = "beers";
		TagField<String> idField = Field.tag(jsonField(Beers.FIELD_ID.getName())).as(Beers.FIELD_ID.getName()).build();
		TextField<String> nameField = Field.text(jsonField(Beers.FIELD_NAME.getName())).as(Beers.FIELD_NAME.getName())
				.build();
		TextField<String> styleField = Field.text(jsonField(Beers.FIELD_STYLE_NAME.getName())).build();
		connection.sync().ftCreate(index, CreateOptions.<String, String>builder().on(DataType.JSON).build(), idField,
				nameField, styleField);
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
		connection.close();
		client.close();
	}

	private String jsonField(String name) {
		return "$." + name;
	}

}
