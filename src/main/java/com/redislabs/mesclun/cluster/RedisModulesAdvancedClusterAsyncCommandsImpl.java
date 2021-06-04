package com.redislabs.mesclun.cluster;

import com.redislabs.mesclun.RedisModulesAsyncCommandsImpl;
import com.redislabs.mesclun.api.async.RedisModulesAsyncCommands;
import com.redislabs.mesclun.cluster.api.StatefulRedisModulesClusterConnection;
import com.redislabs.mesclun.cluster.api.async.RedisModulesAdvancedClusterAsyncCommands;
import com.redislabs.mesclun.gears.Execution;
import com.redislabs.mesclun.gears.ExecutionDetails;
import com.redislabs.mesclun.gears.ExecutionMode;
import com.redislabs.mesclun.gears.Registration;
import com.redislabs.mesclun.gears.output.ExecutionResults;
import com.redislabs.mesclun.search.AggregateOptions;
import com.redislabs.mesclun.search.AggregateResults;
import com.redislabs.mesclun.search.AggregateWithCursorResults;
import com.redislabs.mesclun.search.CreateOptions;
import com.redislabs.mesclun.search.Cursor;
import com.redislabs.mesclun.search.Field;
import com.redislabs.mesclun.search.SearchOptions;
import com.redislabs.mesclun.search.SearchResults;
import com.redislabs.mesclun.search.SugaddOptions;
import com.redislabs.mesclun.search.Suggestion;
import com.redislabs.mesclun.search.SuggetOptions;
import com.redislabs.mesclun.timeseries.Aggregation;
import com.redislabs.mesclun.timeseries.Label;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.MultiNodeExecution;
import io.lettuce.core.cluster.RedisAdvancedClusterAsyncCommandsImpl;
import io.lettuce.core.codec.RedisCodec;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class RedisModulesAdvancedClusterAsyncCommandsImpl<K, V> extends RedisAdvancedClusterAsyncCommandsImpl<K, V> implements RedisModulesAdvancedClusterAsyncCommands<K, V> {

    private final RedisModulesAsyncCommandsImpl<K, V> delegate;

    public RedisModulesAdvancedClusterAsyncCommandsImpl(StatefulRedisModulesClusterConnection<K, V> connection, RedisCodec<K, V> codec) {
        super(connection, codec);
        this.delegate = new RedisModulesAsyncCommandsImpl<>(connection, codec);
    }

    @Override
    public RedisModulesAdvancedClusterAsyncCommands<K, V> getConnection(String nodeId) {
        return (RedisModulesAdvancedClusterAsyncCommands<K, V>) super.getConnection(nodeId);
    }

    @Override
    public RedisModulesAdvancedClusterAsyncCommands<K, V> getConnection(String host, int port) {
        return (RedisModulesAdvancedClusterAsyncCommands<K, V>) super.getConnection(host, port);
    }

    @Override
    public StatefulRedisModulesClusterConnection<K, V> getStatefulConnection() {
        return (StatefulRedisModulesClusterConnection<K, V>) super.getStatefulConnection();
    }

    @Override
    public RedisFuture<String> abortExecution(String id) {
        return delegate.abortExecution(id);
    }

    @Override
    public RedisFuture<List<V>> configGet(K... keys) {
        return delegate.configGet(keys);
    }

    @Override
    public RedisFuture<List<V>> configSet(Map<K, V> map) {
        return delegate.configSet(map);
    }

    @Override
    public RedisFuture<String> dropExecution(String id) {
        return delegate.dropExecution(id);
    }

    @Override
    public RedisFuture<List<Execution>> dumpExecutions() {
        return delegate.dumpExecutions();
    }

    @Override
    public RedisFuture<List<Registration>> dumpRegistrations() {
        return delegate.dumpRegistrations();
    }

    @Override
    public RedisFuture<ExecutionDetails> getExecution(String id) {
        return delegate.getExecution(id);
    }

    @Override
    public RedisFuture<ExecutionDetails> getExecution(String id, ExecutionMode mode) {
        return delegate.getExecution(id, mode);
    }

    @Override
    public RedisFuture<ExecutionResults> getResults(String id) {
        return delegate.getResults(id);
    }

    @Override
    public RedisFuture<ExecutionResults> getResultsBlocking(String id) {
        return delegate.getResultsBlocking(id);
    }

    @Override
    public RedisFuture<ExecutionResults> pyExecute(String function, V... requirements) {
        return delegate.pyExecute(function, requirements);
    }

    @Override
    public RedisFuture<String> pyExecuteUnblocking(String function, V... requirements) {
        return delegate.pyExecuteUnblocking(function, requirements);
    }

    @Override
    public RedisFuture<List<Object>> trigger(String trigger, V... args) {
        return delegate.trigger(trigger, args);
    }

    @Override
    public RedisFuture<String> unregister(String id) {
        return delegate.unregister(id);
    }

    @Override
    public RedisFuture<String> create(K index, Field... fields) {
        return create(index, null, fields);
    }

    @Override
    public RedisFuture<String> create(K index, CreateOptions<K, V> options, Field... fields) {
        return MultiNodeExecution.firstOfAsync(executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).create(index, options, fields)));
    }

    @Override
    public RedisFuture<String> dropIndex(K index) {
        return MultiNodeExecution.firstOfAsync(executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).dropIndex(index)));
    }

    @Override
    public RedisFuture<String> dropIndexDeleteDocs(K index) {
        return MultiNodeExecution.firstOfAsync(executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).dropIndexDeleteDocs(index)));
    }

    @Override
    public RedisFuture<String> alter(K index, Field field) {
        return MultiNodeExecution.firstOfAsync(executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).alter(index, field)));
    }

    @Override
    public RedisFuture<List<Object>> indexInfo(K index) {
        return delegate.indexInfo(index);
    }

    @Override
    public RedisFuture<String> aliasAdd(K name, K index) {
        return MultiNodeExecution.firstOfAsync(executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).aliasAdd(name, index)));
    }

    @Override
    public RedisFuture<String> aliasUpdate(K name, K index) {
        return MultiNodeExecution.firstOfAsync(executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).aliasUpdate(name, index)));
    }

    @Override
    public RedisFuture<String> aliasDel(K name) {
        return MultiNodeExecution.firstOfAsync(executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).aliasDel(name)));
    }

    @Override
    public RedisFuture<List<K>> list() {
        return delegate.list();
    }

    @Override
    public RedisFuture<SearchResults<K, V>> search(K index, V query) {
        return delegate.search(index, query);
    }

    @Override
    public RedisFuture<SearchResults<K, V>> search(K index, V query, SearchOptions options) {
        return delegate.search(index, query, options);
    }

    @Override
    public RedisFuture<AggregateResults<K>> aggregate(K index, V query) {
        return delegate.aggregate(index, query);
    }

    @Override
    public RedisFuture<AggregateResults<K>> aggregate(K index, V query, AggregateOptions options) {
        return delegate.aggregate(index, query, options);
    }

    @Override
    public RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor) {
        return delegate.aggregate(index, query, cursor);
    }

    @Override
    public RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor, AggregateOptions options) {
        return delegate.aggregate(index, query, cursor, options);
    }

    @Override
    public RedisFuture<AggregateWithCursorResults<K>> cursorRead(K index, long cursor) {
        return delegate.cursorRead(index, cursor);
    }

    @Override
    public RedisFuture<AggregateWithCursorResults<K>> cursorRead(K index, long cursor, long count) {
        return delegate.cursorRead(index, cursor, count);
    }

    @Override
    public RedisFuture<String> cursorDelete(K index, long cursor) {
        return delegate.cursorDelete(index, cursor);
    }

    @Override
    public RedisFuture<List<V>> tagVals(K index, K field) {
        return delegate.tagVals(index, field);
    }

    @Override
    public RedisFuture<Long> sugadd(K key, V string, double score) {
        return delegate.sugadd(key, string, score);
    }

    @Override
    public RedisFuture<Long> sugadd(K key, V string, double score, SugaddOptions<V> options) {
        return delegate.sugadd(key, string, score, options);
    }

    @Override
    public RedisFuture<List<Suggestion<V>>> sugget(K key, V prefix) {
        return delegate.sugget(key, prefix);
    }

    @Override
    public RedisFuture<List<Suggestion<V>>> sugget(K key, V prefix, SuggetOptions options) {
        return delegate.sugget(key, prefix, options);
    }

    @Override
    public RedisFuture<Boolean> sugdel(K key, V string) {
        return delegate.sugdel(key, string);
    }

    @Override
    public RedisFuture<Long> suglen(K key) {
        return delegate.suglen(key);
    }

    @Override
    public RedisFuture<Long> dictadd(K dict, V... terms) {
        return MultiNodeExecution.firstOfAsync(executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K,V>)commands).dictadd(dict, terms)));
    }

    @Override
    public RedisFuture<Long> dictdel(K dict, V... terms) {
        return MultiNodeExecution.firstOfAsync(executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K,V>)commands).dictdel(dict, terms)));
    }

    @Override
    public RedisFuture<List<V>> dictdump(K dict) {
        return delegate.dictdump(dict);
    }

    @Override
    public RedisFuture<String> create(K key, Label<K, V>... labels) {
        return delegate.create(key, labels);
    }

    @Override
    public RedisFuture<String> create(K key, com.redislabs.mesclun.timeseries.CreateOptions options, Label<K, V>... labels) {
        return delegate.create(key, options, labels);
    }

    @Override
    public RedisFuture<Long> add(K key, long timestamp, double value, Label<K, V>... labels) {
        return delegate.add(key, timestamp, value, labels);
    }

    @Override
    public RedisFuture<Long> add(K key, long timestamp, double value, com.redislabs.mesclun.timeseries.CreateOptions options, Label<K, V>... labels) {
        return delegate.add(key, timestamp, value, options, labels);
    }

    @Override
    public RedisFuture<String> createRule(K sourceKey, K destKey, Aggregation aggregationType, long timeBucket) {
        return delegate.createRule(sourceKey, destKey, aggregationType, timeBucket);
    }

    @Override
    public RedisFuture<String> deleteRule(K sourceKey, K destKey) {
        return delegate.deleteRule(sourceKey, destKey);
    }
}
