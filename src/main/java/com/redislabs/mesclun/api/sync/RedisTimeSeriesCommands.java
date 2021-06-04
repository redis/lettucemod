package com.redislabs.mesclun.api.sync;

import com.redislabs.mesclun.timeseries.Aggregation;
import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;

public interface RedisTimeSeriesCommands<K,V> {

    @SuppressWarnings("unchecked")
    String create(K key, Label<K, V>... labels);

    @SuppressWarnings("unchecked")
    String create(K key, CreateOptions options, Label<K, V>... labels);

    @SuppressWarnings("unchecked")
    Long add(K key, long timestamp, double value, Label<K, V>... labels);

    @SuppressWarnings("unchecked")
    Long add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels);

    String createRule(K sourceKey, K destKey, Aggregation aggregationType, long timeBucket);

    String deleteRule(K sourceKey, K destKey);
}
