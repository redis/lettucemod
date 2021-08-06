package com.redislabs.lettucemod.cluster.api.sync;

import com.redislabs.lettucemod.api.sync.RedisModulesCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

public interface RedisModulesClusterCommands<K, V> extends RedisClusterCommands<K, V>, RedisModulesCommands<K, V> {
}
