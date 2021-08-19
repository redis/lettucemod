package com.redislabs.lettucemod;

import com.redislabs.lettucemod.api.reactive.RedisModulesReactiveCommands;
import com.redislabs.lettucemod.api.StatefulRedisModulesConnection;
import com.redislabs.lettucemod.gears.*;
import com.redislabs.lettucemod.gears.output.ExecutionResults;
import com.redislabs.lettucemod.search.*;
import com.redislabs.lettucemod.timeseries.Aggregation;
import com.redislabs.lettucemod.timeseries.CreateOptions;
import com.redislabs.lettucemod.timeseries.Label;
import com.redislabs.lettucemod.timeseries.RedisTimeSeriesCommandBuilder;
import io.lettuce.core.RedisReactiveCommandsImpl;
import io.lettuce.core.codec.RedisCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

@SuppressWarnings("unchecked")
public class RedisModulesReactiveCommandsImpl<K, V> extends RedisReactiveCommandsImpl<K, V> implements RedisModulesReactiveCommands<K, V> {

    private final StatefulRedisModulesConnection<K, V> connection;
    private final RedisTimeSeriesCommandBuilder<K, V> timeSeriesCommandBuilder;
    private final RedisGearsCommandBuilder<K, V> gearsCommandBuilder;
    private final RediSearchCommandBuilder<K, V> searchCommandBuilder;

    public RedisModulesReactiveCommandsImpl(StatefulRedisModulesConnection<K, V> connection, RedisCodec<K, V> codec) {
        super(connection, codec);
        this.connection = connection;
        this.gearsCommandBuilder = new RedisGearsCommandBuilder<>(codec);
        this.timeSeriesCommandBuilder = new RedisTimeSeriesCommandBuilder<>(codec);
        this.searchCommandBuilder = new RediSearchCommandBuilder<>(codec);
    }

    @Override
    public StatefulRedisModulesConnection<K, V> getStatefulConnection() {
        return connection;
    }

    @Override
    public Mono<ExecutionResults> pyExecute(String function, V... requirements) {
        return createMono(() -> gearsCommandBuilder.pyExecute(function, requirements));
    }

    @Override
    public Mono<String> pyExecuteUnblocking(String function, V... requirements) {
        return createMono(() -> gearsCommandBuilder.pyExecuteUnblocking(function, requirements));
    }

    @Override
    public Flux<Object> trigger(String trigger, V... args) {
        return createDissolvingFlux(() -> gearsCommandBuilder.trigger(trigger, args));
    }

    @Override
    public Mono<String> unregister(String id) {
        return createMono(() -> gearsCommandBuilder.unregister(id));
    }

    @Override
    public Mono<String> abortExecution(String id) {
        return createMono(() -> gearsCommandBuilder.abortExecution(id));
    }

    @Override
    public Flux<V> configGet(K... keys) {
        return createDissolvingFlux(() -> gearsCommandBuilder.configGet(keys));
    }

    @Override
    public Flux<V> configSet(Map<K, V> map) {
        return createDissolvingFlux(() -> gearsCommandBuilder.configSet(map));
    }

    @Override
    public Mono<String> dropExecution(String id) {
        return createMono(() -> gearsCommandBuilder.dropExecution(id));
    }

    @Override
    public Flux<Execution> dumpExecutions() {
        return createDissolvingFlux(gearsCommandBuilder::dumpExecutions);
    }

    @Override
    public Flux<Registration> dumpRegistrations() {
        return createDissolvingFlux(gearsCommandBuilder::dumpRegistrations);
    }

    @Override
    public Mono<ExecutionDetails> getExecution(String id) {
        return createMono(() -> gearsCommandBuilder.getExecution(id));
    }

    @Override
    public Mono<ExecutionDetails> getExecution(String id, ExecutionMode mode) {
        return createMono(() -> gearsCommandBuilder.getExecution(id, mode));
    }

    @Override
    public Mono<ExecutionResults> getResults(String id) {
        return createMono(() -> gearsCommandBuilder.getResults(id));
    }

    @Override
    public Mono<ExecutionResults> getResultsBlocking(String id) {
        return createMono(() -> gearsCommandBuilder.getResultsBlocking(id));
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


    @Override
    public Mono<String> create(K index, Field... fields) {
        return create(index, null, fields);
    }

    @Override
    public Mono<String> create(K index, com.redislabs.lettucemod.search.CreateOptions<K, V> options, Field... fields) {
        return createMono(() -> searchCommandBuilder.create(index, options, fields));
    }

    @Override
    public Mono<String> dropIndex(K index) {
        return createMono(() -> searchCommandBuilder.dropIndex(index, false));
    }

    @Override
    public Mono<String> dropIndexDeleteDocs(K index) {
        return createMono(() -> searchCommandBuilder.dropIndex(index, true));
    }

    @Override
    public Flux<Object> indexInfo(K index) {
        return createDissolvingFlux(() -> searchCommandBuilder.info(index));
    }

    @Override
    public Mono<SearchResults<K, V>> search(K index, V query) {
        return createMono(() -> searchCommandBuilder.search(index, query, null));
    }

    @Override
    public Mono<SearchResults<K, V>> search(K index, V query, SearchOptions<K, V> options) {
        return createMono(() -> searchCommandBuilder.search(index, query, options));
    }

    @Override
    public Mono<AggregateResults<K>> aggregate(K index, V query) {
        return createMono(() -> searchCommandBuilder.aggregate(index, query, null));
    }

    @Override
    public Mono<AggregateResults<K>> aggregate(K index, V query, AggregateOptions<K, V> options) {
        return createMono(() -> searchCommandBuilder.aggregate(index, query, options));
    }

    @Override
    public Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor) {
        return createMono(() -> searchCommandBuilder.aggregate(index, query, cursor, null));
    }

    @Override
    public Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor, AggregateOptions<K, V> options) {
        return createMono(() -> searchCommandBuilder.aggregate(index, query, cursor, options));
    }

    @Override
    public Mono<AggregateWithCursorResults<K>> cursorRead(K index, long cursor) {
        return createMono(() -> searchCommandBuilder.cursorRead(index, cursor, null));
    }

    @Override
    public Mono<AggregateWithCursorResults<K>> cursorRead(K index, long cursor, long count) {
        return createMono(() -> searchCommandBuilder.cursorRead(index, cursor, count));
    }

    @Override
    public Mono<String> cursorDelete(K index, long cursor) {
        return createMono(() -> searchCommandBuilder.cursorDelete(index, cursor));
    }

    @Override
    public Mono<Long> sugadd(K key, V string, double score) {
        return createMono(() -> searchCommandBuilder.sugadd(key, string, score));
    }

    @Override
    public Mono<Long> sugadd(K key, V string, double score, SugaddOptions<K, V> options) {
        return createMono(() -> searchCommandBuilder.sugadd(key, string, score, options));
    }

    @Override
    public Flux<Suggestion<V>> sugget(K key, V prefix) {
        return createDissolvingFlux(() -> searchCommandBuilder.sugget(key, prefix));
    }

    @Override
    public Flux<Suggestion<V>> sugget(K key, V prefix, SuggetOptions options) {
        return createDissolvingFlux(() -> searchCommandBuilder.sugget(key, prefix, options));
    }

    @Override
    public Mono<Boolean> sugdel(K key, V string) {
        return createMono(() -> searchCommandBuilder.sugdel(key, string));
    }

    @Override
    public Mono<Long> suglen(K key) {
        return createMono(() -> searchCommandBuilder.suglen(key));
    }

    @Override
    public Mono<String> alter(K index, Field field) {
        return createMono(() -> searchCommandBuilder.alter(index, field));
    }

    @Override
    public Mono<String> aliasAdd(K name, K index) {
        return createMono(() -> searchCommandBuilder.aliasAdd(name, index));
    }

    @Override
    public Mono<String> aliasUpdate(K name, K index) {
        return createMono(() -> searchCommandBuilder.aliasUpdate(name, index));
    }

    @Override
    public Mono<String> aliasDel(K name) {
        return createMono(() -> searchCommandBuilder.aliasDel(name));
    }

    @Override
    public Flux<K> list() {
        return createDissolvingFlux(searchCommandBuilder::list);
    }

    @Override
    public Flux<V> tagVals(K index, K field) {
        return createDissolvingFlux(() -> searchCommandBuilder.tagVals(index, field));
    }

    @Override
    public Mono<Long> dictadd(K dict, V... terms) {
        return createMono(() -> searchCommandBuilder.dictadd(dict, terms));
    }

    @Override
    public Mono<Long> dictdel(K dict, V... terms) {
        return createMono(() -> searchCommandBuilder.dictdel(dict, terms));
    }

    @Override
    public Flux<V> dictdump(K dict) {
        return createDissolvingFlux(() -> searchCommandBuilder.dictdump(dict));
    }
}
