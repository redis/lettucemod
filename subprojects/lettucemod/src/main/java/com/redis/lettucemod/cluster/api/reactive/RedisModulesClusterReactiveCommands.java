package com.redis.lettucemod.cluster.api.reactive;

import com.redis.lettucemod.api.reactive.RedisModulesReactiveCommands;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;

public interface RedisModulesClusterReactiveCommands<K, V> extends RedisClusterReactiveCommands<K, V>, RedisModulesReactiveCommands<K, V> {
}
