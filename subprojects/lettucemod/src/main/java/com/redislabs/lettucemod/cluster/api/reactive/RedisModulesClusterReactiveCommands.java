package com.redislabs.lettucemod.cluster.api.reactive;

import com.redislabs.lettucemod.api.reactive.RedisModulesReactiveCommands;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;

public interface RedisModulesClusterReactiveCommands<K, V> extends RedisClusterReactiveCommands<K, V>, RedisModulesReactiveCommands<K, V> {
}
