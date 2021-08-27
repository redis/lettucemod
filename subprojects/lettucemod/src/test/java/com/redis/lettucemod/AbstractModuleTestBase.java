package com.redis.lettucemod;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.reactive.RedisModulesReactiveCommands;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.RedisModulesContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.support.enterprise.rest.Database;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Testcontainers
public abstract class AbstractModuleTestBase {

    @Container
    private static final RedisModulesContainer REDIS = new RedisModulesContainer("preview");
    @Container
    private static final RedisEnterpriseContainer REDIS_ENTERPRISE = new RedisEnterpriseContainer().withModules(Database.Module.GEARS, Database.Module.SEARCH, Database.Module.TIMESERIES, Database.Module.JSON).withOSSCluster();

    static Stream<RedisServer> redisServers() {
        return Stream.of(REDIS, REDIS_ENTERPRISE);
    }

    protected final Map<RedisServer, AbstractRedisClient> clients = new HashMap<>();
    protected final Map<RedisServer, StatefulRedisModulesConnection<String, String>> connections = new HashMap<>();
    protected final Map<RedisServer, RedisModulesCommands<String, String>> syncs = new HashMap<>();
    protected final Map<RedisServer, RedisModulesAsyncCommands<String, String>> asyncs = new HashMap<>();
    protected final Map<RedisServer, RedisModulesReactiveCommands<String, String>> reactives = new HashMap<>();

    @BeforeEach
    public void setupEach() {
        for (RedisServer redis : redisServers().collect(Collectors.toList())) {
            if (redis.isCluster()) {
                RedisModulesClusterClient client = RedisModulesClusterClient.create(redis.getRedisURI());
                put(redis, client, client.connect());
            } else {
                RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
                put(redis, client, client.connect());
            }
        }
    }

    private void put(RedisServer redis, AbstractRedisClient client, StatefulRedisModulesConnection<String, String> connection) {
        clients.put(redis, client);
        connections.put(redis, connection);
        syncs.put(redis, connection.sync());
        asyncs.put(redis, connection.async());
        reactives.put(redis, connection.reactive());
    }

    @AfterEach
    public void cleanupEach() {
        for (RedisModulesCommands<String, String> sync : syncs.values()) {
            sync.flushall();
        }
        for (StatefulConnection<String, String> connection : connections.values()) {
            connection.close();
        }
        for (AbstractRedisClient client : clients.values()) {
            client.shutdown();
            client.getResources().shutdown();
        }
    }

    protected RedisModulesCommands<String, String> sync(RedisServer redis) {
        return syncs.get(redis);
    }

    protected RedisModulesAsyncCommands<String, String> async(RedisServer redis) {
        return asyncs.get(redis);
    }

    protected RedisModulesReactiveCommands<String, String> reactive(RedisServer redis) {
        return reactives.get(redis);
    }

}
