package com.redislabs.lettucemod.cluster.api;

import com.redislabs.lettucemod.api.StatefulRedisModulesConnection;
import com.redislabs.lettucemod.cluster.api.async.RedisModulesAdvancedClusterAsyncCommands;
import com.redislabs.lettucemod.cluster.api.reactive.RedisModulesAdvancedClusterReactiveCommands;
import com.redislabs.lettucemod.cluster.api.sync.RedisModulesAdvancedClusterCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

public interface StatefulRedisModulesClusterConnection<K, V> extends StatefulRedisClusterConnection<K, V>, StatefulRedisModulesConnection<K, V> {

    RedisModulesAdvancedClusterCommands<K, V> sync();

    RedisModulesAdvancedClusterAsyncCommands<K, V> async();

    RedisModulesAdvancedClusterReactiveCommands<K, V> reactive();
}
