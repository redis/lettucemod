package com.redislabs.mesclun;

import com.redislabs.mesclun.api.StatefulRedisModulesConnection;
import com.redislabs.mesclun.cluster.RedisModulesClusterClient;
import com.redislabs.testcontainers.RedisEnterpriseContainer;
import com.redislabs.testcontainers.support.enterprise.rest.Database;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class TestRedisModulesClusterClient {

    @Container
    private static final RedisEnterpriseContainer REDIS = new RedisEnterpriseContainer().withModules(Database.Module.SEARCH).withOSSCluster();

    @Test
    public void testCreate() {
        testPing(RedisModulesClusterClient.create(REDIS.getRedisURI()).connect());
        testPing(RedisModulesClusterClient.create(DefaultClientResources.create(), RedisURI.create(REDIS.getRedisURI())).connect());
        testPing(RedisModulesClusterClient.create(REDIS.getRedisURI()).connect(StringCodec.UTF8));
    }

    private void testPing(StatefulRedisModulesConnection<String, String> connection) {
        Assertions.assertEquals("PONG", connection.reactive().ping().block());
    }

}
