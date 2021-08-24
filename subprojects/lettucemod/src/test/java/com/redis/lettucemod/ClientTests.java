package com.redis.lettucemod;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.testcontainers.RedisModulesContainer;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ClientTests {

    @Container
    private static final RedisModulesContainer REDISMOD = new RedisModulesContainer();

    @Test
    public void testCreate() {
        testPing(RedisModulesClient.create().connect(RedisURI.create(REDISMOD.getRedisURI())));
        testPing(RedisModulesClient.create(DefaultClientResources.create()).connect(RedisURI.create(REDISMOD.getRedisURI())));
        testPing(RedisModulesClient.create(DefaultClientResources.create(), REDISMOD.getRedisURI()).connect());
        testPing(RedisModulesClient.create(DefaultClientResources.create(), RedisURI.create(REDISMOD.getRedisURI())).connect());
        testPing(RedisModulesClient.create().connect(StringCodec.UTF8, RedisURI.create(REDISMOD.getRedisURI())));
    }

    private void testPing(StatefulRedisModulesConnection<String, String> connection) {
        Assertions.assertEquals("PONG", connection.reactive().ping().block());
    }

}
