package com.redis.lettucemod.cluster.api.sync;

import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

public interface RedisModulesAdvancedClusterCommands<K, V> extends RedisAdvancedClusterCommands<K, V>, RedisModulesClusterCommands<K, V> {

    RedisModulesClusterCommands<K, V> getConnection(String nodeId);

    RedisModulesClusterCommands<K, V> getConnection(String host, int port);

    /**
     * @return the underlying connection.
     */
    StatefulRedisModulesClusterConnection<K, V> getStatefulConnection();
}
