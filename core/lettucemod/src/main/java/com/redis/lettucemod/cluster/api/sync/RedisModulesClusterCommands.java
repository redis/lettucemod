package com.redis.lettucemod.cluster.api.sync;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

public interface RedisModulesClusterCommands<K, V> extends RedisClusterCommands<K, V>, RedisModulesCommands<K, V> {
}
