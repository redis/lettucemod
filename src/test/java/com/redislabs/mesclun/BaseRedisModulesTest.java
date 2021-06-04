package com.redislabs.mesclun;

import com.redislabs.mesclun.api.StatefulRedisModulesConnection;
import com.redislabs.mesclun.api.async.RedisModulesAsyncCommands;
import com.redislabs.mesclun.api.reactive.RedisModulesReactiveCommands;
import com.redislabs.mesclun.api.sync.RedisModulesCommands;
import com.redislabs.mesclun.cluster.RedisModulesClusterClient;
import com.redislabs.mesclun.cluster.api.StatefulRedisModulesClusterConnection;
import com.redislabs.testcontainers.RedisEnterpriseContainer;
import com.redislabs.testcontainers.RedisModulesContainer;
import com.redislabs.testcontainers.RedisServer;
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
public class BaseRedisModulesTest {

    @Container
    private static final RedisModulesContainer REDIS = new RedisModulesContainer();
    private static final RedisServer REDIS_ENTERPRISE = new RedisServer() {
        @Override
        public String getRedisURI() {
            return "redis://redis-12000.jrx.demo.redislabs.com:12000";
        }

        @Override
        public boolean isCluster() {
            return true;
        }
    };

    static Stream<RedisServer> redisServers() {
        return Stream.of(REDIS_ENTERPRISE);
    }

    protected Map<RedisServer, AbstractRedisClient> clients = new HashMap<>();
    protected Map<RedisServer, StatefulConnection<String, String>> connections = new HashMap<>();
    protected Map<RedisServer, RedisModulesCommands<String, String>> syncs = new HashMap<>();
    protected Map<RedisServer, RedisModulesAsyncCommands<String, String>> asyncs = new HashMap<>();
    protected Map<RedisServer, RedisModulesReactiveCommands<String, String>> reactives = new HashMap<>();

    @BeforeEach
    public void setupEach() {
        for (RedisServer redis : redisServers().collect(Collectors.toList())) {
            if (redis.isCluster()) {
                RedisModulesClusterClient client = RedisModulesClusterClient.create(redis.getRedisURI());
                clients.put(redis, client);
                StatefulRedisModulesClusterConnection<String, String> connection = client.connect();
                connections.put(redis, connection);
                syncs.put(redis, connection.sync());
                asyncs.put(redis, connection.async());
                reactives.put(redis, connection.reactive());
            } else {
                RedisModulesClient client = RedisModulesClient.create(redis.getRedisURI());
                clients.put(redis, client);
                StatefulRedisModulesConnection<String, String> connection = client.connect();
                connections.put(redis, connection);
                syncs.put(redis, connection.sync());
                asyncs.put(redis, connection.async());
                reactives.put(redis, connection.reactive());
            }
        }
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
