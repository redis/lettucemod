package com.redislabs.mesclun;

import io.lettuce.core.api.StatefulRedisConnection;

public interface StatefulRedisModulesConnection<K, V> extends StatefulRedisConnection<K, V> {

	RedisModulesCommands<K, V> sync();

	RedisModulesAsyncCommands<K, V> async();

	RedisModulesReactiveCommands<K, V> reactive();
}