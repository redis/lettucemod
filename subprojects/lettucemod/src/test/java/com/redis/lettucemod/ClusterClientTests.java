package com.redis.lettucemod;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.testcontainers.RedisEnterpriseContainer;
import com.redis.testcontainers.support.enterprise.rest.Database;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ClusterClientTests {

    @Container
    private static final RedisEnterpriseContainer REDIS_ENTERPRISE_CLUSTER = new RedisEnterpriseContainer().withModules(Database.Module.SEARCH).withOSSCluster();

    @Test
    public void testCreate() {
        testPing(RedisModulesClusterClient.create(REDIS_ENTERPRISE_CLUSTER.getRedisURI()).connect());
        testPing(RedisModulesClusterClient.create(DefaultClientResources.create(), RedisURI.create(REDIS_ENTERPRISE_CLUSTER.getRedisURI())).connect());
        testPing(RedisModulesClusterClient.create(REDIS_ENTERPRISE_CLUSTER.getRedisURI()).connect(StringCodec.UTF8));
    }

    private void testPing(StatefulRedisModulesConnection<String, String> connection) {
        Assertions.assertEquals("PONG", connection.reactive().ping().block());
    }

}
