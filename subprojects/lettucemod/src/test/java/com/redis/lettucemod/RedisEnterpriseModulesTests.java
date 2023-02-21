package com.redis.lettucemod;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.springframework.util.unit.DataSize;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
public class RedisEnterpriseModulesTests extends BaseModulesTests {

	private final RedisEnterpriseContainer redisContainer = new RedisEnterpriseContainer(
			RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag("latest"))
			.withDatabase(Database.name("ModulesTests").memory(DataSize.ofMegabytes(110)).ossCluster(true)
					.modules(RedisModule.SEARCH, RedisModule.JSON, RedisModule.GEARS, RedisModule.TIMESERIES).build());

	@Override
	protected RedisServer getRedisServer() {
		return redisContainer;
	}

}
