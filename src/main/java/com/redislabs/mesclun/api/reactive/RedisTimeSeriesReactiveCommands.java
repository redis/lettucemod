package com.redislabs.mesclun.api.reactive;

import com.redislabs.mesclun.timeseries.Aggregation;
import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;
import reactor.core.publisher.Mono;

public interface RedisTimeSeriesReactiveCommands<K, V> {

    @SuppressWarnings("unchecked")
    Mono<String> create(K key, Label<K, V>... labels);

    @SuppressWarnings("unchecked")
    Mono<String> create(K key, CreateOptions options, Label<K, V>... labels);

    @SuppressWarnings("unchecked")
    Mono<Long> add(K key, long timestamp, double value, Label<K, V>... labels);

    @SuppressWarnings("unchecked")
    Mono<Long> add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels);

    Mono<String> createRule(K sourceKey, K destKey, Aggregation aggregationType, long timeBucket);

    Mono<String> deleteRule(K sourceKey, K destKey);

}
