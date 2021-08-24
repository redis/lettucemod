package com.redis.lettucemod.api.async;

import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.Label;
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
