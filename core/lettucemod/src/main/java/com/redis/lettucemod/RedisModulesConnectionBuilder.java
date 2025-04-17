package com.redis.lettucemod;

import java.util.function.Supplier;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class RedisModulesConnectionBuilder {

    private final AbstractRedisClient client;

    private ReadFrom readFrom;

    public RedisModulesConnectionBuilder(AbstractRedisClient client) {
        this.client = client;
    }

    public static RedisModulesConnectionBuilder client(AbstractRedisClient client) {
        return new RedisModulesConnectionBuilder(client);
    }

    public RedisModulesConnectionBuilder readFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
        return this;
    }

    public StatefulRedisModulesConnection<String, String> connection() {
        return connection(StringCodec.UTF8);
    }

    public StatefulRedisPubSubConnection<String, String> pubSubConnection() {
        return pubSubConnection(StringCodec.UTF8);
    }

    public <K, V> StatefulRedisPubSubConnection<K, V> pubSubConnection(RedisCodec<K, V> codec) {
        if (client instanceof RedisModulesClusterClient) {
            return ((RedisModulesClusterClient) client).connectPubSub(codec);
        }
        return ((RedisModulesClient) client).connectPubSub(codec);
    }

    public <K, V> Supplier<StatefulRedisModulesConnection<K, V>> connectionSupplier(RedisCodec<K, V> codec) {
        return () -> connection(codec);
    }

    public <K, V> StatefulRedisModulesConnection<K, V> connection(RedisCodec<K, V> codec) {
        if (client instanceof RedisModulesClusterClient) {
            StatefulRedisModulesClusterConnection<K, V> connection = ((RedisModulesClusterClient) client).connect(codec);
            if (readFrom != null) {
                connection.setReadFrom(readFrom);
            }
            return connection;
        }
        return ((RedisModulesClient) client).connect(codec);
    }

}
