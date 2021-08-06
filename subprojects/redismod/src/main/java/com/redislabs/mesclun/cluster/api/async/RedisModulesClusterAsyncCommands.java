package com.redislabs.mesclun.cluster.api.async;

import com.redislabs.mesclun.api.async.RedisModulesAsyncCommands;
import com.redislabs.mesclun.cluster.api.StatefulRedisModulesClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;

public interface RedisModulesClusterAsyncCommands<K, V> extends RedisClusterAsyncCommands<K, V>, RedisModulesAsyncCommands<K, V> {

    StatefulRedisModulesClusterConnection<K, V> getStatefulConnection();
}
