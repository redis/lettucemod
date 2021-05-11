package com.redislabs.mesclun;

import com.redislabs.testcontainers.RedisModulesContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class BaseRedisModulesTest {

    @Container
    protected static final RedisModulesContainer REDIS = new RedisModulesContainer();
    protected RedisModulesClient client;
    protected StatefulRedisModulesConnection<String, String> connection;
    protected RedisModulesCommands<String, String> sync;
    protected RedisModulesAsyncCommands<String, String> async;
    protected RedisModulesReactiveCommands<String, String> reactive;

    @BeforeEach
    public void setupEach() {
        client = RedisModulesClient.create(REDIS.getRedisURI());
        connection = client.connect();
        sync = connection.sync();
        async = connection.async();
        reactive = connection.reactive();
    }

    @AfterEach
    public void cleanupEach() {
        sync.flushall();
        connection.close();
        client.shutdown();
        client.getResources().shutdown();
    }

}
