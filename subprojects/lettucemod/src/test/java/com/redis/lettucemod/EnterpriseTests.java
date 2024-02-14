package com.redis.lettucemod;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.springframework.util.unit.DataSize;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.testcontainers.AbstractRedisContainer;
import com.redis.testcontainers.RedisEnterpriseContainer;

import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;

@EnabledOnOs(OS.LINUX)
class EnterpriseTests extends ModulesTests {

	private final RedisEnterpriseContainer container = new RedisEnterpriseContainer(
			RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag("latest"))
			.withDatabase(Database.name("ModulesTests").memory(DataSize.ofMegabytes(110)).ossCluster(true)
					.modules(RedisModule.SEARCH, RedisModule.JSON, RedisModule.TIMESERIES).build());

	@Override
	protected AbstractRedisContainer<?> getRedisContainer() {
		return container;
	}

	@Test
	void client() {
		try (RedisModulesClusterClient client = RedisModulesClusterClient.create(container.getRedisURI());
				StatefulRedisModulesClusterConnection<String, String> connection = client.connect();) {
			assertPing(connection);
		}
		DefaultClientResources resources = DefaultClientResources.create();
		try (RedisModulesClusterClient client = RedisModulesClusterClient.create(resources,
				RedisURI.create(container.getRedisURI()));
				StatefulRedisModulesClusterConnection<String, String> connection = client.connect();) {
			assertPing(connection);
		}
		resources.shutdown();
		try (RedisModulesClusterClient client = RedisModulesClusterClient.create(container.getRedisURI());
				StatefulRedisModulesClusterConnection<String, String> connection = client.connect(StringCodec.UTF8);) {
			assertPing(connection);
		}
	}

}
