package com.redislabs.mesclun.cluster.api.reactive;

import com.redislabs.mesclun.api.reactive.RedisModulesReactiveCommands;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;

public interface RedisModulesClusterReactiveCommands<K, V> extends RedisClusterReactiveCommands<K, V>, RedisModulesReactiveCommands<K, V> {
}
