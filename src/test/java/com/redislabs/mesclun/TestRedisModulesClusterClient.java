package com.redislabs.mesclun;

import com.redislabs.mesclun.api.StatefulRedisModulesConnection;
import com.redislabs.mesclun.cluster.RedisModulesClusterClient;
import com.redislabs.testcontainers.RedisModulesContainer;
import com.redislabs.testcontainers.RedisServer;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRedisModulesClusterClient {

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

    @Test
    public void testCreate() {
        testPing(RedisModulesClusterClient.create(REDIS_ENTERPRISE.getRedisURI()).connect());
        testPing(RedisModulesClusterClient.create(DefaultClientResources.create(), RedisURI.create(REDIS_ENTERPRISE.getRedisURI())).connect());
        testPing(RedisModulesClusterClient.create(REDIS_ENTERPRISE.getRedisURI()).connect(StringCodec.UTF8));
    }

    private void testPing(StatefulRedisModulesConnection<String, String> connection) {
        Assertions.assertEquals("PONG", connection.reactive().ping().block());
    }

}
