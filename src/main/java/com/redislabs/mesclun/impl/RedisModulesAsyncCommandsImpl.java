package com.redislabs.mesclun.impl;

import com.redislabs.mesclun.StatefulRedisModulesConnection;
import com.redislabs.mesclun.RedisModulesAsyncCommands;
import com.redislabs.mesclun.gears.*;
import com.redislabs.mesclun.gears.output.ExecutionResults;
import com.redislabs.mesclun.timeseries.Aggregation;
import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;
import com.redislabs.mesclun.timeseries.RedisTimeSeriesCommandBuilder;
import io.lettuce.core.RedisAsyncCommandsImpl;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.codec.RedisCodec;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class RedisModulesAsyncCommandsImpl<K, V> extends RedisAsyncCommandsImpl<K, V> implements RedisModulesAsyncCommands<K, V> {

    private final StatefulRedisModulesConnection<K, V> connection;
    private final RedisGearsCommandBuilder<K, V> gearsCommandBuilder;
    private final RedisTimeSeriesCommandBuilder<K, V> timeSeriesCommandBuilder;

    public RedisModulesAsyncCommandsImpl(StatefulRedisModulesConnection<K, V> connection, RedisCodec<K, V> codec) {
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
    public RedisFuture<String> abortExecution(String id) {
        return dispatch(gearsCommandBuilder.abortExecution(id));
    }

    @Override
    public RedisFuture<List<V>> configGet(K... keys) {
        return dispatch(gearsCommandBuilder.configGet(keys));
    }

    @Override
    public RedisFuture<List<V>> configSet(Map<K, V> map) {
        return dispatch(gearsCommandBuilder.configSet(map));
    }

    @Override
    public RedisFuture<String> dropExecution(String id) {
        return dispatch(gearsCommandBuilder.dropExecution(id));
    }

    @Override
    public RedisFuture<List<Execution>> dumpExecutions() {
        return dispatch(gearsCommandBuilder.dumpExecutions());
    }

    @Override
    public RedisFuture<ExecutionResults> pyExecute(String function, V... requirements) {
        return dispatch(gearsCommandBuilder.pyExecute(function, requirements));
    }

    @Override
    public RedisFuture<String> pyExecuteUnblocking(String function, V... requirements) {
        return dispatch(gearsCommandBuilder.pyExecuteUnblocking(function, requirements));
    }

    @Override
    public RedisFuture<List<Object>> trigger(String trigger, V... args) {
        return dispatch(gearsCommandBuilder.trigger(trigger, args));
    }

    @Override
    public RedisFuture<String> unregister(String id) {
        return dispatch(gearsCommandBuilder.unregister(id));
    }

    @Override
    public RedisFuture<List<Registration>> dumpRegistrations() {
        return dispatch(gearsCommandBuilder.dumpRegistrations());
    }

    @Override
    public RedisFuture<ExecutionDetails> getExecution(String id) {
        return dispatch(gearsCommandBuilder.getExecution(id));
    }

    @Override
    public RedisFuture<ExecutionDetails> getExecution(String id, ExecutionMode mode) {
        return dispatch(gearsCommandBuilder.getExecution(id, mode));
    }

    @Override
    public RedisFuture<ExecutionResults> getResults(String id) {
        return dispatch(gearsCommandBuilder.getResults(id));
    }

    @Override
    public RedisFuture<ExecutionResults> getResultsBlocking(String id) {
        return dispatch(gearsCommandBuilder.getResultsBlocking(id));
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
