package com.redis.lettucemod.utils;

import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.enterprise.Database;
import com.redis.enterprise.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisServer;

@EnabledOnOs(OS.LINUX)
class EnterpriseTests extends AbstractConnectionBuilderTests {

    private final Database database = Database.builder().name("ModulesTests").memoryMB(50).ossCluster(true).build();

    @SuppressWarnings("resource")
    private final RedisEnterpriseContainer container = new RedisEnterpriseContainer(
            RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag(RedisEnterpriseContainer.DEFAULT_TAG)).withDatabase(database);

    @Override
    protected RedisServer getRedisServer() {
        return container;
    }

}
