package com.redislabs.mesclun.cluster.api.sync;

import com.redislabs.mesclun.api.sync.RedisModulesCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;

public interface RedisModulesClusterCommands<K, V> extends RedisClusterCommands<K, V>, RedisModulesCommands<K, V> {
}
