package com.redislabs.mesclun.api.async;

import com.redislabs.mesclun.timeseries.Aggregation;
import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;
import io.lettuce.core.RedisFuture;

public interface RedisTimeSeriesAsyncCommands<K, V> {

    @SuppressWarnings("unchecked")
    RedisFuture<String> create(K key, Label<K, V>... labels);

    @SuppressWarnings("unchecked")
    RedisFuture<String> create(K key, CreateOptions options, Label<K, V>... labels);

    @SuppressWarnings("unchecked")
    RedisFuture<Long> add(K key, long timestamp, double value, Label<K, V>... labels);

    @SuppressWarnings("unchecked")
    RedisFuture<Long> add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels);

    RedisFuture<String> createRule(K sourceKey, K destKey, Aggregation aggregationType, long timeBucket);

    RedisFuture<String> deleteRule(K sourceKey, K destKey);

}
