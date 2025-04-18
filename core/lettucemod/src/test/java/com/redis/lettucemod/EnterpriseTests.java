package com.redis.lettucemod;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.enterprise.testcontainers.RedisEnterpriseContainer;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;

@EnabledOnOs(OS.LINUX)
class EnterpriseTests extends ModulesTests {

    private final Database database = Database.builder().name("ModulesTests").memoryMB(50).ossCluster(true)
            .modules(RedisModule.SEARCH, RedisModule.JSON, RedisModule.PROBABILISTIC, RedisModule.TIMESERIES).build();

    @SuppressWarnings("resource")
    private final RedisEnterpriseContainer container = new RedisEnterpriseContainer(
            RedisEnterpriseContainer.DEFAULT_IMAGE_NAME.withTag(RedisEnterpriseContainer.DEFAULT_TAG)).withDatabase(database);

    @Override
    protected RedisServer getRedisServer() {
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
