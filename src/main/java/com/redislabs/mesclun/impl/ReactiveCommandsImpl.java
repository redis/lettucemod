package com.redislabs.mesclun.impl;

import com.redislabs.mesclun.RedisModulesReactiveCommands;
import com.redislabs.mesclun.StatefulRedisModulesConnection;
import com.redislabs.mesclun.gears.PyExecuteOptions;
import com.redislabs.mesclun.gears.RedisGearsCommandBuilder;
import com.redislabs.mesclun.gears.Registration;
import com.redislabs.mesclun.timeseries.Aggregation;
import com.redislabs.mesclun.timeseries.CreateOptions;
import com.redislabs.mesclun.timeseries.Label;
import com.redislabs.mesclun.timeseries.RedisTimeSeriesCommandBuilder;
import io.lettuce.core.RedisReactiveCommandsImpl;
import io.lettuce.core.codec.RedisCodec;
import reactor.core.publisher.Mono;

import java.util.List;

public class ReactiveCommandsImpl<K, V> extends RedisReactiveCommandsImpl<K, V> implements RedisModulesReactiveCommands<K, V> {

    private final StatefulRedisModulesConnection<K, V> connection;
    private final RedisTimeSeriesCommandBuilder<K, V> timeSeriesCommandBuilder;
    private final RedisGearsCommandBuilder<K, V> gearsCommandBuilder;

    public ReactiveCommandsImpl(StatefulRedisModulesConnection<K, V> connection, RedisCodec<K, V> codec) {
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
    public Mono<String> pyExecute(String function, PyExecuteOptions options) {
        return createMono(() -> gearsCommandBuilder.pyExecute(function, options));
    }

    @Override
    public Mono<List<Registration>> dumpRegistrations() {
        return createMono(() -> gearsCommandBuilder.dumpRegistrations());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<String> create(K key, CreateOptions options, Label<K, V>... labels) {
        return createMono(() -> timeSeriesCommandBuilder.create(key, options, labels));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<String> create(K key, Label<K, V>... labels) {
        return createMono(() -> timeSeriesCommandBuilder.create(key, null, labels));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Long> add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels) {
        return createMono(() -> timeSeriesCommandBuilder.add(key, timestamp, value, options, labels));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Mono<Long> add(K key, long timestamp, double value, Label<K, V>... labels) {
        return createMono(() -> timeSeriesCommandBuilder.add(key, timestamp, value, null, labels));
    }

    @Override
    public Mono<String> createRule(K sourceKey, K destKey, Aggregation aggregationType, long timeBucket) {
        return createMono(() -> timeSeriesCommandBuilder.createRule(sourceKey, destKey, aggregationType, timeBucket));
    }

    @Override
    public Mono<String> deleteRule(K sourceKey, K destKey) {
        return createMono(() -> timeSeriesCommandBuilder.deleteRule(sourceKey, destKey));
    }

}
