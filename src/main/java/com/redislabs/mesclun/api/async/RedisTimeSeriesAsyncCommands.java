package com.redislabs.mesclun.api.async;

import com.redislabs.mesclun.api.StatefulRedisTimeSeriesConnection;
import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;

public interface RedisTimeSeriesAsyncCommands<K, V> extends RedisAsyncCommands<K, V> {

	StatefulRedisTimeSeriesConnection<K, V> getStatefulConnection();

	@SuppressWarnings("unchecked")
	RedisFuture<String> create(K key, Label<K, V>... labels);

	@SuppressWarnings("unchecked")
	RedisFuture<String> create(K key, CreateOptions options, Label<K, V>... labels);

	@SuppressWarnings("unchecked")
	RedisFuture<Long> add(K key, long timestamp, double value, Label<K, V>... labels);

	@SuppressWarnings("unchecked")
	RedisFuture<Long> add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels);

}
