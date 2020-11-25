package com.redislabs.mesclun.timeseries.impl;

import com.redislabs.mesclun.RedisTimeSeriesCommandBuilder;
import com.redislabs.mesclun.api.StatefulRedisTimeSeriesConnection;
import com.redislabs.mesclun.api.async.RedisTimeSeriesAsyncCommands;
import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;

import io.lettuce.core.RedisAsyncCommandsImpl;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.codec.RedisCodec;

public class AsyncCommandsImpl<K, V> extends RedisAsyncCommandsImpl<K, V>
		implements RedisTimeSeriesAsyncCommands<K, V> {

	private final StatefulRedisTimeSeriesConnection<K, V> connection;
	private final RedisTimeSeriesCommandBuilder<K, V> commandBuilder;

	public AsyncCommandsImpl(StatefulRedisTimeSeriesConnection<K, V> connection,
			RedisCodec<K, V> codec) {
		super(connection, codec);
		this.connection = connection;
		this.commandBuilder = new RedisTimeSeriesCommandBuilder<>(codec);
	}

	@Override
	public StatefulRedisTimeSeriesConnection<K, V> getStatefulConnection() {
		return connection;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public RedisFuture<String> create(K key, Label<K, V>... labels) {
		return dispatch(commandBuilder.create(key, null, labels));
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public RedisFuture<String> create(K key, CreateOptions options, Label<K, V>... labels) {
		return dispatch(commandBuilder.create(key, options, labels));
	}

	@SuppressWarnings("unchecked")
	@Override
	public RedisFuture<Long> add(K key, long timestamp, double value, Label<K, V>... labels) {
		return dispatch(commandBuilder.add(key, timestamp, value, null, labels));
	}

	@SuppressWarnings("unchecked")
	@Override
	public RedisFuture<Long> add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels) {
		return dispatch(commandBuilder.add(key, timestamp, value, options, labels));
	}

}
