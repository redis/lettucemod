package com.redis.lettucemod.test;

import java.util.Arrays;
import java.util.Collection;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisEnterpriseContainer.RedisModule;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.jupiter.AbstractTestcontainersRedisTestBase;

@Testcontainers
public abstract class AbstractLettuceModTestBase extends AbstractTestcontainersRedisTestBase {

	@Container
	protected static final RedisModulesContainer REDISMOD = new RedisModulesContainer();

	@Container
	protected static final RedisEnterpriseContainer REDIS_ENTERPRISE = new RedisEnterpriseContainer()
			.withModules(RedisModule.GEARS, RedisModule.SEARCH, RedisModule.TIMESERIES, RedisModule.JSON)
			.withOSSCluster();

	@Override
	protected Collection<RedisServer> servers() {
		return Arrays.asList(REDISMOD, REDIS_ENTERPRISE);
	}
}
