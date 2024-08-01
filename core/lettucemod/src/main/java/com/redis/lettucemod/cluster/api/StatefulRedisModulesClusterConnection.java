package com.redis.lettucemod.cluster.api;

import com.redis.lettucemod.cluster.api.async.RedisModulesAdvancedClusterAsyncCommands;
import com.redis.lettucemod.cluster.api.reactive.RedisModulesAdvancedClusterReactiveCommands;
import com.redis.lettucemod.cluster.api.sync.RedisModulesAdvancedClusterCommands;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

public interface StatefulRedisModulesClusterConnection<K, V> extends StatefulRedisClusterConnection<K, V>, StatefulRedisModulesConnection<K, V> {

    RedisModulesAdvancedClusterCommands<K, V> sync();

    RedisModulesAdvancedClusterAsyncCommands<K, V> async();

    RedisModulesAdvancedClusterReactiveCommands<K, V> reactive();
}
