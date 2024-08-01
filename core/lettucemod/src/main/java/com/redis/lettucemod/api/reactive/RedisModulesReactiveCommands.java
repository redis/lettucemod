package com.redis.lettucemod.api.reactive;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.api.reactive.RedisReactiveCommands;

public interface RedisModulesReactiveCommands<K, V>
		extends RedisReactiveCommands<K, V>, RedisGearsReactiveCommands<K, V>, RedisJSONReactiveCommands<K, V>,
		RediSearchReactiveCommands<K, V>, RedisTimeSeriesReactiveCommands<K, V>, RedisBloomReactiveCommands<K,V>{

	@Override
	StatefulRedisModulesConnection<K, V> getStatefulConnection();

}
