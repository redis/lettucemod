package com.redislabs.mesclun.api;

import com.redislabs.mesclun.api.async.RedisModulesAsyncCommands;
import com.redislabs.mesclun.api.reactive.RedisModulesReactiveCommands;
import com.redislabs.mesclun.api.sync.RedisModulesCommands;
import io.lettuce.core.api.StatefulRedisConnection;

public interface StatefulRedisModulesConnection<K, V> extends StatefulRedisConnection<K, V> {

	RedisModulesCommands<K, V> sync();

	RedisModulesAsyncCommands<K, V> async();

	RedisModulesReactiveCommands<K, V> reactive();
}