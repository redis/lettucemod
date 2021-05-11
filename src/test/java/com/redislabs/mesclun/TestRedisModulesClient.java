package com.redislabs.mesclun;

import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRedisModulesClient extends BaseRedisModulesTest {

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
