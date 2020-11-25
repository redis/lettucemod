package com.redislabs.mesclun.timeseries.impl;

import com.redislabs.mesclun.RedisTimeSeriesCommandBuilder;
import com.redislabs.mesclun.api.StatefulRedisTimeSeriesConnection;
import com.redislabs.mesclun.api.reactive.RedisTimeSeriesReactiveCommands;
import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;

import io.lettuce.core.RedisReactiveCommandsImpl;
import io.lettuce.core.codec.RedisCodec;
import reactor.core.publisher.Mono;

public class ReactiveCommandsImpl<K, V> extends RedisReactiveCommandsImpl<K, V>
		implements RedisTimeSeriesReactiveCommands<K, V> {

	private final StatefulRedisTimeSeriesConnection<K, V> connection;
	private final RedisTimeSeriesCommandBuilder<K, V> commandBuilder;

	public ReactiveCommandsImpl(StatefulRedisTimeSeriesConnection<K, V> connection, RedisCodec<K, V> codec) {
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
	public Mono<String> create(K key, CreateOptions options, Label<K, V>... labels) {
		return createMono(() -> commandBuilder.create(key, options, labels));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Mono<String> create(K key, Label<K, V>... labels) {
		return createMono(() -> commandBuilder.create(key, null, labels));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Mono<Long> add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels) {
		return createMono(() -> commandBuilder.add(key, timestamp, value, options, labels));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Mono<Long> add(K key, long timestamp, double value, Label<K, V>... labels) {
		return createMono(() -> commandBuilder.add(key, timestamp, value, null, labels));
	}

}
