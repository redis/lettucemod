package com.redis.lettucemod.utils;

import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.redis.enterprise.Database;
import com.redis.enterprise.testcontainers.RedisEnterpriseServer;
import com.redis.testcontainers.RedisServer;

@EnabledIfEnvironmentVariable(named = RedisEnterpriseServer.ENV_HOST, matches = ".*")
class REServerTests extends AbstractConnectionBuilderTests {

    private final Database database = Database.builder().name("ModulesTests").memoryMB(50).ossCluster(true).port(12001).build();

    @SuppressWarnings("resource")
    private final RedisEnterpriseServer container = new RedisEnterpriseServer().withDatabase(database);

    @Override
    protected RedisServer getRedisServer() {
        return container;
    }

}
