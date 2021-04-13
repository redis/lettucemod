package com.redislabs.mesclun.impl;

import com.redislabs.mesclun.RedisModulesAsyncCommands;
import com.redislabs.mesclun.StatefulRedisModulesConnection;
import com.redislabs.mesclun.gears.PyExecuteOptions;
import com.redislabs.mesclun.gears.RedisGearsCommandBuilder;
import com.redislabs.mesclun.gears.Registration;
import com.redislabs.mesclun.timeseries.Aggregation;
import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;
import com.redislabs.mesclun.timeseries.RedisTimeSeriesCommandBuilder;
import io.lettuce.core.RedisAsyncCommandsImpl;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.codec.RedisCodec;

import java.util.List;

public class AsyncCommandsImpl<K, V> extends RedisAsyncCommandsImpl<K, V> implements RedisModulesAsyncCommands<K, V> {

    private final StatefulRedisModulesConnection<K, V> connection;
    private final RedisGearsCommandBuilder<K, V> gearsCommandBuilder;
    private final RedisTimeSeriesCommandBuilder<K, V> timeSeriesCommandBuilder;

    public AsyncCommandsImpl(StatefulRedisModulesConnection<K, V> connection, RedisCodec<K, V> codec) {
        super(connection, codec);
        this.connection = connection;
        this.gearsCommandBuilder = new RedisGearsCommandBuilder<>(codec);
        this.timeSeriesCommandBuilder = new RedisTimeSeriesCommandBuilder<>(codec);
    }

    @Override
    public StatefulRedisModulesConnection<K, V> getStatefulConnection() {
        return connection;
    }

    @Override
    public RedisFuture<String> pyExecute(String function, PyExecuteOptions options) {
        return dispatch(gearsCommandBuilder.pyExecute(function, options));
    }

    @Override
    public RedisFuture<List<Registration>> dumpRegistrations() {
        return dispatch(gearsCommandBuilder.dumpRegistrations());
    }

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<String> create(K key, Label<K, V>... labels) {
        return dispatch(timeSeriesCommandBuilder.create(key, null, labels));
    }

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<String> create(K key, CreateOptions options, Label<K, V>... labels) {
        return dispatch(timeSeriesCommandBuilder.create(key, options, labels));
    }

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<Long> add(K key, long timestamp, double value, Label<K, V>... labels) {
        return dispatch(timeSeriesCommandBuilder.add(key, timestamp, value, null, labels));
    }

    @SuppressWarnings("unchecked")
    @Override
    public RedisFuture<Long> add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels) {
        return dispatch(timeSeriesCommandBuilder.add(key, timestamp, value, options, labels));
    }

    @Override
    public RedisFuture<String> createRule(K sourceKey, K destKey, Aggregation aggregationType, long timeBucket) {
        return dispatch(timeSeriesCommandBuilder.createRule(sourceKey, destKey, aggregationType, timeBucket));
    }

    @Override
    public RedisFuture<String> deleteRule(K sourceKey, K destKey) {
        return dispatch(timeSeriesCommandBuilder.deleteRule(sourceKey, destKey));
    }
}
