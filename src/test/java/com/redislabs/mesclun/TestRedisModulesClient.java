package com.redislabs.mesclun;

import com.redislabs.mesclun.api.StatefulRedisModulesConnection;
import com.redislabs.testcontainers.RedisModulesContainer;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class TestRedisModulesClient {

    @Container
    private static final RedisModulesContainer REDIS = new RedisModulesContainer();

    @Test
    public void testCreate() {
        testPing(RedisModulesClient.create().connect(RedisURI.create(REDIS.getRedisURI())));
        testPing(RedisModulesClient.create(DefaultClientResources.create()).connect(RedisURI.create(REDIS.getRedisURI())));
        testPing(RedisModulesClient.create(DefaultClientResources.create(), REDIS.getRedisURI()).connect());
        testPing(RedisModulesClient.create(DefaultClientResources.create(), RedisURI.create(REDIS.getRedisURI())).connect());
        testPing(RedisModulesClient.create().connect(StringCodec.UTF8, RedisURI.create(REDIS.getRedisURI())));
    }

    private void testPing(StatefulRedisModulesConnection<String, String> connection) {
        Assertions.assertEquals("PONG", connection.reactive().ping().block());
    }

}
