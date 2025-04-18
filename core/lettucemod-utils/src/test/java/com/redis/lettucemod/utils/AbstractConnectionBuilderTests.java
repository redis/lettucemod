package com.redis.lettucemod.utils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startable;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.codec.ByteArrayCodec;

@Testcontainers
@TestInstance(Lifecycle.PER_CLASS)
abstract class AbstractConnectionBuilderTests {

    protected StatefulRedisModulesConnection<String, String> connection;

    private AbstractRedisClient client;

    protected abstract RedisServer getRedisServer();

    @BeforeAll
    void setup() {
        RedisServer server = getRedisServer();
        if (server instanceof Startable) {
            ((Startable) server).start();
        }
        String uri = server.getRedisURI();
        if (server.isRedisCluster()) {
            RedisModulesClusterClient clusterClient = RedisModulesClusterClient.create(uri);
            client = clusterClient;
            connection = clusterClient.connect();
        } else {
            RedisModulesClient redisClient = RedisModulesClient.create(uri);
            client = redisClient;
            connection = redisClient.connect();
        }
    }

    @AfterAll
    void teardown() {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
            client.getResources().shutdown();
        }
        RedisServer server = getRedisServer();
        if (server instanceof Startable) {
            ((Startable) server).stop();
        }
    }

    @BeforeEach
    void setupRedis() {
        connection.sync().flushall();
    }

    @Test
    void testConnection() {
        try (StatefulRedisModulesConnection<String, String> connection = ConnectionBuilder.client(client).connection()) {
            Assertions.assertEquals("PONG", connection.sync().ping());
        }
        try (StatefulRedisModulesConnection<byte[], byte[]> connection = ConnectionBuilder.client(client)
                .connection(ByteArrayCodec.INSTANCE)) {
            Assertions.assertEquals("PONG", connection.sync().ping());
        }
    }

}
