package com.redis.lettucemod.utils;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.resource.ClientResources;

public class ClientBuilder {

    private final RedisURI uri;

    private boolean cluster;

    private ClientOptions options;

    private ClientResources resources;

    public ClientBuilder(RedisURI uri) {
        this.uri = uri;
    }

    public AbstractRedisClient build() {
        if (cluster) {
            RedisModulesClusterClient client = clusterClient();
            if (options != null) {
                client.setOptions((ClusterClientOptions) options);
            }
            return client;
        }
        RedisModulesClient client = client();
        if (options != null) {
            client.setOptions(options);
        }
        return client;
    }

    private RedisModulesClient client() {
        if (resources == null) {
            return RedisModulesClient.create(uri);
        }
        return RedisModulesClient.create(resources, uri);
    }

    private RedisModulesClusterClient clusterClient() {
        if (resources == null) {
            return RedisModulesClusterClient.create(uri);
        }
        return RedisModulesClusterClient.create(resources, uri);
    }

    public ClientBuilder cluster() {
        return cluster(true);
    }

    public ClientBuilder cluster(boolean cluster) {
        this.cluster = cluster;
        return this;
    }

    public ClientBuilder resources(ClientResources resources) {
        this.resources = resources;
        return this;
    }

    public ClientBuilder options(ClientOptions options) {
        this.options = options;
        return this;
    }

    public static ClientBuilder of(RedisURI uri) {
        return new ClientBuilder(uri);
    }

}
