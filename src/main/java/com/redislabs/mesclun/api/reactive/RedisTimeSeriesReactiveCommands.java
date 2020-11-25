package com.redislabs.mesclun.api.reactive;

import com.redislabs.mesclun.api.StatefulRedisTimeSeriesConnection;
import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;

import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Mono;

public interface RedisTimeSeriesReactiveCommands<K, V> extends RedisReactiveCommands<K, V> {

	StatefulRedisTimeSeriesConnection<K, V> getStatefulConnection();

	@SuppressWarnings("unchecked")
	Mono<String> create(K key, Label<K, V>... labels);

	@SuppressWarnings("unchecked")
	Mono<String> create(K key, CreateOptions options, Label<K, V>... labels);

	@SuppressWarnings("unchecked")
	Mono<Long> add(K key, long timestamp, double value, Label<K, V>... labels);

	@SuppressWarnings("unchecked")
	Mono<Long> add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels);

}
