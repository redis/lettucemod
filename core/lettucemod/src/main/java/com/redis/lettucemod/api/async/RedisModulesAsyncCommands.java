package com.redis.lettucemod.api.async;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.api.async.RedisAsyncCommands;

public interface RedisModulesAsyncCommands<K, V> extends RedisAsyncCommands<K, V>, RedisGearsAsyncCommands<K, V>,
		RedisJSONAsyncCommands<K, V>, RediSearchAsyncCommands<K, V>, RedisTimeSeriesAsyncCommands<K, V>, RedisBloomAsyncCommands<K,V>{

	@Override
	StatefulRedisModulesConnection<K, V> getStatefulConnection();

}
