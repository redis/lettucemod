package com.redislabs.mesclun.api.sync;

import com.redislabs.mesclun.api.StatefulRedisTimeSeriesConnection;
import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;

import io.lettuce.core.api.sync.RedisCommands;

public interface RedisTimeSeriesCommands<K, V> extends RedisCommands<K, V> {

	StatefulRedisTimeSeriesConnection<K, V> getStatefulConnection();

	@SuppressWarnings("unchecked")
	String create(K key, Label<K, V>... labels);

	@SuppressWarnings("unchecked")
	String create(K key, CreateOptions options, Label<K, V>... labels);

	@SuppressWarnings("unchecked")
	Long add(K key, long timestamp, double value, Label<K, V>... labels);

	@SuppressWarnings("unchecked")
	Long add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels);
}
