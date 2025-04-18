package com.redis.lettucemod.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.resource.ClientResources;

public class ClientBuilderTests {

    @Test
    void testBasics() {
        Assertions.assertInstanceOf(RedisModulesClient.class, builder().build());
        Assertions.assertInstanceOf(RedisModulesClusterClient.class, builder().cluster(true).build());
    }

    @Test
    void testOptions() {
        ClientOptions options = ClientOptions.builder().autoReconnect(false).build();
        AbstractRedisClient client = builder().options(options).build();
        Assertions.assertEquals(options, client.getOptions());
    }

    @Test
    void testClusterOptions() {
        ClusterClientOptions options = ClusterClientOptions.builder().autoReconnect(false).build();
        AbstractRedisClient client = builder().cluster().options(options).build();
        Assertions.assertEquals(options, client.getOptions());
    }

    @Test
    void testResources() {
        ClientResources resources = ClientResources.builder().ioThreadPoolSize(3).build();
        AbstractRedisClient client = builder().resources(resources).build();
        Assertions.assertEquals(resources, client.getResources());
    }

    private ClientBuilder builder() {
        return ClientBuilder.of(RedisURI.create("redis://localhost:12345"));
    }

}
