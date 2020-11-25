package com.redislabs.mesclun.api;

import com.redislabs.mesclun.api.async.RedisTimeSeriesAsyncCommands;
import com.redislabs.mesclun.api.reactive.RedisTimeSeriesReactiveCommands;
import com.redislabs.mesclun.api.sync.RedisTimeSeriesCommands;

import io.lettuce.core.api.StatefulRedisConnection;

public interface StatefulRedisTimeSeriesConnection<K, V> extends StatefulRedisConnection<K, V> {

	RedisTimeSeriesCommands<K, V> sync();

	RedisTimeSeriesAsyncCommands<K, V> async();

	RedisTimeSeriesReactiveCommands<K, V> reactive();
}