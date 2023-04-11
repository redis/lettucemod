package com.redis.lettucemod;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.api.sync.RedisTimeSeriesCommands;
import com.redis.lettucemod.timeseries.DuplicatePolicy;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.util.ClientBuilder;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.lettucemod.util.RedisURIBuilder;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.RedisStackContainer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.AclSetuserArgs;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;

class RedisStackTests extends AbstractTests {

	private final RedisStackContainer container = new RedisStackContainer(
			RedisStackContainer.DEFAULT_IMAGE_NAME.withTag(RedisStackContainer.DEFAULT_TAG));

	@Override
	protected RedisServer getRedisServer() {
		return container;
	}

	@Test
	void getPath() throws JsonProcessingException {
		String json = "{\"a\":2, \"b\": 3, \"nested\": {\"a\": 4, \"b\": null}}";
		RedisModulesCommands<String, String> sync = connection.sync();
		sync.jsonSet("doc", "$", json);
		assertEquals("[3,null]", sync.jsonGet("doc", "$..b"));
		assertJSONEquals("{\"$..b\":[3,null],\"..a\":[2,4]}", sync.jsonGet("doc", "..a", "$..b"));
	}

	@Test
	void credentials() {
		String username = "alice";
		String password = "ecila";
		connection.sync().aclSetuser(username,
				AclSetuserArgs.Builder.on().addPassword(password).allCommands().allKeys());
		try (AbstractRedisClient client = ClientBuilder
				.create(RedisURIBuilder.create(getRedisServer().getRedisURI()).username(username)
						.password("wrongpassword".toCharArray()).build())
				.cluster(getRedisServer().isCluster()).build()) {
			RedisModulesUtils.connection(client);
			Assertions.fail("Expected connection failure");
		} catch (Exception e) {
			// expected
		}
		String key = "foo";
		String value = "bar";
		RedisURI uri = RedisURIBuilder.create(getRedisServer().getRedisURI()).username(username)
				.password(password.toCharArray()).build();
		try (AbstractRedisClient client = ClientBuilder.create(uri).cluster(getRedisServer().isCluster()).build();
				StatefulRedisModulesConnection<String, String> connection = RedisModulesUtils.connection(client)) {
			connection.sync().set(key, value);
			Assertions.assertEquals(value, connection.sync().get(key));
		}
	}

	@Test
	void tsMget() {
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
	void tsCreate() {
		assertEquals("OK", connection.sync().tsCreate(TS_KEY,
				com.redis.lettucemod.timeseries.CreateOptions.<String, String>builder().retentionPeriod(6000).build()));
		String key = "virag";
		List<Label<String, String>> labels = Arrays.asList(Label.of("name", "value"));
		assertEquals("OK",
				connection.sync().tsCreate(key, com.redis.lettucemod.timeseries.CreateOptions.<String, String>builder()
						.retentionPeriod(100000L).labels(labels).policy(DuplicatePolicy.LAST).build()));
		List<GetResult<String, String>> results = connection.sync().tsMgetWithLabels("name=value");
		Label<String, String> expectedLabel = labels.get(0);
		assertEquals(expectedLabel, results.get(0).getLabels().get(0));
	}

	@Test
	void client() {
		DefaultClientResources resources = DefaultClientResources.create();
		try (RedisModulesClient client = RedisModulesClient.create();
				StatefulRedisModulesConnection<String, String> connection = client
						.connect(RedisURI.create(container.getRedisURI()))) {
			ping(connection);
		}
		try (RedisModulesClient client = RedisModulesClient.create(resources);
				StatefulRedisModulesConnection<String, String> connection = client
						.connect(RedisURI.create(container.getRedisURI()))) {
			ping(connection);
		}
		try (RedisModulesClient client = RedisModulesClient.create(resources, container.getRedisURI());
				StatefulRedisModulesConnection<String, String> connection = client.connect()) {
			ping(connection);
		}
		try (RedisModulesClient client = RedisModulesClient.create(resources, RedisURI.create(container.getRedisURI()));
				StatefulRedisModulesConnection<String, String> connection = client.connect()) {
			ping(connection);
		}
		try (RedisModulesClient client = RedisModulesClient.create();
				StatefulRedisModulesConnection<String, String> connection = client.connect(StringCodec.UTF8,
						RedisURI.create(container.getRedisURI()))) {
			ping(connection);
		}
		resources.shutdown();
	}

}
