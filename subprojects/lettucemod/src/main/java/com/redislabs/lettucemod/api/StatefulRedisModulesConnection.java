package com.redislabs.lettucemod.api;

import com.redislabs.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redislabs.lettucemod.api.reactive.RedisModulesReactiveCommands;
import com.redislabs.lettucemod.api.sync.RedisModulesCommands;
import io.lettuce.core.api.StatefulRedisConnection;

public interface StatefulRedisModulesConnection<K, V> extends StatefulRedisConnection<K, V> {

	RedisModulesCommands<K, V> sync();

	RedisModulesAsyncCommands<K, V> async();

	RedisModulesReactiveCommands<K, V> reactive();
}
