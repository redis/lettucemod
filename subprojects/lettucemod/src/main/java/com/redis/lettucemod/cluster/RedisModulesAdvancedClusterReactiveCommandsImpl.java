package com.redis.lettucemod.cluster;

import com.redis.lettucemod.cluster.api.reactive.RedisModulesAdvancedClusterReactiveCommands;
import com.redis.lettucemod.search.*;
import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.RedisModulesReactiveCommandsImpl;
import com.redis.lettucemod.api.reactive.RedisModulesReactiveCommands;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.ExecutionMode;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.gears.output.ExecutionResults;
import io.lettuce.core.cluster.RedisAdvancedClusterReactiveCommandsImpl;
import io.lettuce.core.codec.RedisCodec;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;

@SuppressWarnings("unchecked")
public class RedisModulesAdvancedClusterReactiveCommandsImpl<K, V> extends RedisAdvancedClusterReactiveCommandsImpl<K, V> implements RedisModulesAdvancedClusterReactiveCommands<K, V> {

    private final RedisModulesReactiveCommandsImpl<K, V> delegate;

    public RedisModulesAdvancedClusterReactiveCommandsImpl(StatefulRedisModulesClusterConnection<K, V> connection, RedisCodec<K, V> codec) {
        super(connection, codec);
        this.delegate = new RedisModulesReactiveCommandsImpl<>(connection, codec);
    }

    @Override
    public RedisModulesAdvancedClusterReactiveCommands<K, V> getConnection(String nodeId) {
        return (RedisModulesAdvancedClusterReactiveCommands<K, V>) super.getConnection(nodeId);
    }

    @Override
    public RedisModulesAdvancedClusterReactiveCommands<K, V> getConnection(String host, int port) {
        return (RedisModulesAdvancedClusterReactiveCommands<K, V>) super.getConnection(host, port);
    }

    @Override
    public StatefulRedisModulesClusterConnection<K, V> getStatefulConnection() {
        return (StatefulRedisModulesClusterConnection<K, V>) super.getStatefulConnection();
    }

    @Override
    public Mono<String> abortExecution(String id) {
        return delegate.abortExecution(id);
    }

    @Override
    public Flux<V> configGet(K... keys) {
        return delegate.configGet(keys);
    }

    @Override
    public Flux<V> configSet(Map<K, V> map) {
        return delegate.configSet(map);
    }

    @Override
    public Mono<String> dropExecution(String id) {
        return delegate.dropExecution(id);
    }

    @Override
    public Flux<Execution> dumpExecutions() {
        return delegate.dumpExecutions();
    }

    @Override
    public Flux<Registration> dumpRegistrations() {
        return delegate.dumpRegistrations();
    }

    @Override
    public Mono<ExecutionDetails> getExecution(String id) {
        return delegate.getExecution(id);
    }

    @Override
    public Mono<ExecutionDetails> getExecution(String id, ExecutionMode mode) {
        return delegate.getExecution(id, mode);
    }

    @Override
    public Mono<ExecutionResults> getResults(String id) {
        return delegate.getResults(id);
    }

    @Override
    public Mono<ExecutionResults> getResultsBlocking(String id) {
        return delegate.getResultsBlocking(id);
    }

    @Override
    public Mono<ExecutionResults> pyExecute(String function, V... requirements) {
        return delegate.pyExecute(function, requirements);
    }

    @Override
    public Mono<String> pyExecuteUnblocking(String function, V... requirements) {
        return delegate.pyExecuteUnblocking(function, requirements);
    }

    @Override
    public Flux<Object> trigger(String trigger, V... args) {
        return delegate.trigger(trigger, args);
    }

    @Override
    public Mono<String> unregister(String id) {
        return delegate.unregister(id);
    }

    @Override
    public Mono<String> create(K index, Field... fields) {
        return create(index, null, fields);
    }

    @Override
    public Mono<String> create(K index, com.redis.lettucemod.search.CreateOptions<K, V> options, Field... fields) {
        Map<String, Publisher<String>> publishers = executeOnUpstream(commands -> ((RedisModulesReactiveCommands<K, V>) commands).create(index, options, fields));
        return Flux.merge(publishers.values()).last();
    }

    @Override
    public Mono<String> dropIndex(K index) {
        Map<String, Publisher<String>> publishers = executeOnUpstream(commands -> ((RedisModulesReactiveCommands<K, V>) commands).dropIndex(index));
        return Flux.merge(publishers.values()).last();
    }

    @Override
    public Mono<String> dropIndexDeleteDocs(K index) {
        Map<String, Publisher<String>> publishers = executeOnUpstream(commands -> ((RedisModulesReactiveCommands<K, V>) commands).dropIndexDeleteDocs(index));
        return Flux.merge(publishers.values()).last();
    }

    @Override
    public Mono<String> alter(K index, Field field) {
        Map<String, Publisher<String>> publishers = executeOnUpstream(commands -> ((RedisModulesReactiveCommands<K, V>) commands).alter(index, field));
        return Flux.merge(publishers.values()).last();
    }

    @Override
    public Flux<Object> indexInfo(K index) {
        return delegate.indexInfo(index);
    }

    @Override
    public Mono<String> aliasAdd(K name, K index) {
        Map<String, Publisher<String>> publishers = executeOnUpstream(commands -> ((RedisModulesReactiveCommands<K, V>) commands).aliasAdd(name, index));
        return Flux.merge(publishers.values()).last();
    }

    @Override
    public Mono<String> aliasUpdate(K name, K index) {
        Map<String, Publisher<String>> publishers = executeOnUpstream(commands -> ((RedisModulesReactiveCommands<K, V>) commands).aliasUpdate(name, index));
        return Flux.merge(publishers.values()).last();
    }

    @Override
    public Mono<String> aliasDel(K name) {
        Map<String, Publisher<String>> publishers = executeOnUpstream(commands -> ((RedisModulesReactiveCommands<K, V>) commands).aliasDel(name));
        return Flux.merge(publishers.values()).last();
    }

    @Override
    public Flux<K> list() {
        return delegate.list();
    }

    @Override
    public Mono<SearchResults<K, V>> search(K index, V query) {
        return delegate.search(index, query);
    }

    @Override
    public Mono<SearchResults<K, V>> search(K index, V query, SearchOptions<K, V> options) {
        return delegate.search(index, query, options);
    }

    @Override
    public Mono<AggregateResults<K>> aggregate(K index, V query) {
        return delegate.aggregate(index, query);
    }

    @Override
    public Mono<AggregateResults<K>> aggregate(K index, V query, AggregateOptions<K, V> options) {
        return delegate.aggregate(index, query, options);
    }

    @Override
    public Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor) {
        return delegate.aggregate(index, query, cursor);
    }

    @Override
    public Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor, AggregateOptions<K, V> options) {
        return delegate.aggregate(index, query, cursor, options);
    }

    @Override
    public Mono<AggregateWithCursorResults<K>> cursorRead(K index, long cursor) {
        return delegate.cursorRead(index, cursor);
    }

    @Override
    public Mono<AggregateWithCursorResults<K>> cursorRead(K index, long cursor, long count) {
        return delegate.cursorRead(index, cursor, count);
    }

    @Override
    public Mono<String> cursorDelete(K index, long cursor) {
        return delegate.cursorDelete(index, cursor);
    }

    @Override
    public Flux<V> tagVals(K index, K field) {
        return delegate.tagVals(index, field);
    }

    @Override
    public Mono<Long> sugadd(K key, V string, double score) {
        return delegate.sugadd(key, string, score);
    }

    @Override
    public Mono<Long> sugadd(K key, V string, double score, SugaddOptions<K, V> options) {
        return delegate.sugadd(key, string, score, options);
    }

    @Override
    public Flux<Suggestion<V>> sugget(K key, V prefix) {
        return delegate.sugget(key, prefix);
    }

    @Override
    public Flux<Suggestion<V>> sugget(K key, V prefix, SuggetOptions options) {
        return delegate.sugget(key, prefix, options);
    }

    @Override
    public Mono<Boolean> sugdel(K key, V string) {
        return delegate.sugdel(key, string);
    }

    @Override
    public Mono<Long> suglen(K key) {
        return delegate.suglen(key);
    }

    @Override
    public Mono<Long> dictadd(K dict, V... terms) {
        Map<String, Publisher<Long>> publishers = executeOnUpstream(commands -> ((RedisModulesReactiveCommands<K, V>) commands).dictadd(dict, terms));
        return Flux.merge(publishers.values()).last();
    }

    @Override
    public Mono<Long> dictdel(K dict, V... terms) {
        Map<String, Publisher<Long>> publishers = executeOnUpstream(commands -> ((RedisModulesReactiveCommands<K, V>) commands).dictdel(dict, terms));
        return Flux.merge(publishers.values()).last();
    }

    @Override
    public Flux<V> dictdump(K dict) {
        return delegate.dictdump(dict);
    }

    @Override
    public Mono<String> create(K key, Label<K, V>... labels) {
        return delegate.create(key, labels);
    }

    @Override
    public Mono<String> create(K key, CreateOptions options, Label<K, V>... labels) {
        return delegate.create(key, options, labels);
    }

    @Override
    public Mono<Long> add(K key, long timestamp, double value, Label<K, V>... labels) {
        return delegate.add(key, timestamp, value, labels);
    }

    @Override
    public Mono<Long> add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels) {
        return delegate.add(key, timestamp, value, options, labels);
    }

    @Override
    public Mono<String> createRule(K sourceKey, K destKey, Aggregation aggregationType, long timeBucket) {
        return delegate.createRule(sourceKey, destKey, aggregationType, timeBucket);
    }

    @Override
    public Mono<String> deleteRule(K sourceKey, K destKey) {
        return delegate.deleteRule(sourceKey, destKey);
    }
}
