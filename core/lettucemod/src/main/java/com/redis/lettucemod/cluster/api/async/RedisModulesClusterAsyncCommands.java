package com.redis.lettucemod.cluster.api.async;

import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

public interface RedisModulesClusterAsyncCommands<K, V> extends RedisClusterAsyncCommands<K, V>, RedisModulesAsyncCommands<K, V> {

    StatefulRedisModulesClusterConnection<K, V> getStatefulConnection();
}
