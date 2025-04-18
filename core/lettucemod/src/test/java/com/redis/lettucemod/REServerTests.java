package com.redis.lettucemod;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.redis.enterprise.Database;
import com.redis.enterprise.RedisModule;
import com.redis.enterprise.testcontainers.RedisEnterpriseServer;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;

@EnabledIfEnvironmentVariable(named = RedisEnterpriseServer.ENV_HOST, matches = ".*")
class REServerTests extends ModulesTests {

    private final Database database = Database.builder().name("ModulesTests").ossCluster(true).port(12001)
            .modules(RedisModule.SEARCH, RedisModule.JSON, RedisModule.PROBABILISTIC, RedisModule.TIMESERIES).build();

    @SuppressWarnings("resource")
    private final RedisEnterpriseServer container = new RedisEnterpriseServer().withDatabase(database);

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
