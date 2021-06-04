package com.redislabs.mesclun.cluster.api.async;

import com.redislabs.mesclun.cluster.api.StatefulRedisModulesClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;

public interface RedisModulesAdvancedClusterAsyncCommands<K, V> extends RedisAdvancedClusterAsyncCommands<K, V>, RedisModulesClusterAsyncCommands<K, V> {

    RedisModulesClusterAsyncCommands<K, V> getConnection(String nodeId);

    RedisModulesClusterAsyncCommands<K, V> getConnection(String host, int port);

    StatefulRedisModulesClusterConnection<K, V> getStatefulConnection();
}
