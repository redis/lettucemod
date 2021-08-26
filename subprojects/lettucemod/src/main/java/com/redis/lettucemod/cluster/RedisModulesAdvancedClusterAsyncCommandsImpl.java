package com.redis.lettucemod.cluster;

import com.redis.lettucemod.RedisModulesAsyncCommandsImpl;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.lettucemod.cluster.api.async.RedisModulesAdvancedClusterAsyncCommands;
import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.ExecutionMode;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.gears.output.ExecutionResults;
import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateResults;
import com.redis.lettucemod.search.AggregateWithCursorResults;
import com.redis.lettucemod.search.Cursor;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.SugaddOptions;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.Label;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.MultiNodeExecution;
import io.lettuce.core.cluster.PipelinedRedisFuture;
import io.lettuce.core.cluster.RedisAdvancedClusterAsyncCommandsImpl;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.KeyValueStreamingChannel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class RedisModulesAdvancedClusterAsyncCommandsImpl<K, V> extends RedisAdvancedClusterAsyncCommandsImpl<K, V> implements RedisModulesAdvancedClusterAsyncCommands<K, V> {

    private final RedisModulesAsyncCommandsImpl<K, V> delegate;
    private final RedisCodec<K, V> codec;

    public RedisModulesAdvancedClusterAsyncCommandsImpl(StatefulRedisModulesClusterConnection<K, V> connection, RedisCodec<K, V> codec) {
        super(connection, codec);
        this.codec = codec;
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
    public RedisFuture<String> create(K index, com.redis.lettucemod.search.CreateOptions<K, V> options, Field... fields) {
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
    public RedisFuture<SearchResults<K, V>> search(K index, V query, SearchOptions<K, V> options) {
        return delegate.search(index, query, options);
    }

    @Override
    public RedisFuture<AggregateResults<K>> aggregate(K index, V query) {
        return delegate.aggregate(index, query);
    }

    @Override
    public RedisFuture<AggregateResults<K>> aggregate(K index, V query, AggregateOptions<K, V> options) {
        return delegate.aggregate(index, query, options);
    }

    @Override
    public RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor) {
        return delegate.aggregate(index, query, cursor);
    }

    @Override
    public RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor, AggregateOptions<K, V> options) {
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
    public RedisFuture<Long> sugadd(K key, V string, double score, SugaddOptions<K, V> options) {
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
        return MultiNodeExecution.firstOfAsync(executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).dictadd(dict, terms)));
    }

    @Override
    public RedisFuture<Long> dictdel(K dict, V... terms) {
        return MultiNodeExecution.firstOfAsync(executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).dictdel(dict, terms)));
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
    public RedisFuture<String> create(K key, CreateOptions options, Label<K, V>... labels) {
        return delegate.create(key, options, labels);
    }

    @Override
    public RedisFuture<Long> add(K key, long timestamp, double value, Label<K, V>... labels) {
        return delegate.add(key, timestamp, value, labels);
    }

    @Override
    public RedisFuture<Long> add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels) {
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

    @Override
    public RedisFuture<Long> del(K key) {
        return delegate.del(key);
    }

    @Override
    public RedisFuture<Long> del(K key, K path) {
        return delegate.del(key, path);
    }

    @Override
    public RedisFuture<V> get(K key, K... paths) {
        return delegate.get(key, paths);
    }

    @Override
    public RedisFuture<V> get(K key, GetOptions<K, V> options, K... paths) {
        return delegate.get(key, options, paths);
    }

    @Override
    public RedisFuture<List<KeyValue<K, V>>> mget(K path, K... keys) {
        return mget(path, Arrays.asList(keys));
    }

    @Override
    public RedisFuture<Long> mget(KeyValueStreamingChannel<K, V> channel, K path, K... keys) {
        return mget(channel, path, Arrays.asList(keys));
    }

    public RedisFuture<List<KeyValue<K, V>>> mget(K path, Iterable<K> keys) {
        Map<Integer, List<K>> partitioned = ModulesSlotHash.partition(codec, keys);

        if (partitioned.size() < 2) {
            return delegate.mget(path, keys);
        }

        Map<K, Integer> slots = ModulesSlotHash.getSlots(partitioned);
        Map<Integer, RedisFuture<List<KeyValue<K, V>>>> executions = new HashMap<>();

        for (Map.Entry<Integer, List<K>> entry : partitioned.entrySet()) {
            RedisFuture<List<KeyValue<K, V>>> mget = delegate.mget(path, entry.getValue());
            executions.put(entry.getKey(), mget);
        }

        // restore order of key
        return new PipelinedRedisFuture<>(executions, objectPipelinedRedisFuture -> {
            List<KeyValue<K, V>> result = new ArrayList<>();
            for (K opKey : keys) {
                int slot = slots.get(opKey);

                int position = partitioned.get(slot).indexOf(opKey);
                RedisFuture<List<KeyValue<K, V>>> listRedisFuture = executions.get(slot);
                result.add(MultiNodeExecution.execute(() -> listRedisFuture.get().get(position)));
            }

            return result;
        });
    }

    public RedisFuture<Long> mget(KeyValueStreamingChannel<K, V> channel, K path, Iterable<K> keys) {
        Map<Integer, List<K>> partitioned = ModulesSlotHash.partition(codec, keys);

        if (partitioned.size() < 2) {
            return delegate.mget(channel, path, keys);
        }

        Map<Integer, RedisFuture<Long>> executions = new HashMap<>();

        for (Map.Entry<Integer, List<K>> entry : partitioned.entrySet()) {
            RedisFuture<Long> del = delegate.mget(channel, path, entry.getValue());
            executions.put(entry.getKey(), del);
        }

        return MultiNodeExecution.aggregateAsync(executions);
    }

    @Override
    public RedisFuture<String> set(K key, K path, V json) {
        return delegate.set(key, path, json);
    }

    @Override
    public RedisFuture<String> setNX(K key, K path, V json) {
        return delegate.setNX(key, path, json);
    }

    @Override
    public RedisFuture<String> setXX(K key, K path, V json) {
        return delegate.setXX(key, path, json);
    }

    @Override
    public RedisFuture<String> type(K key) {
        return delegate.type(key);
    }

    @Override
    public RedisFuture<String> type(K key, K path) {
        return delegate.type(key, path);
    }

    @Override
    public RedisFuture<V> numIncrBy(K key, K path, double number) {
        return delegate.numIncrBy(key, path, number);
    }

    @Override
    public RedisFuture<V> numMultBy(K key, K path, double number) {
        return delegate.numMultBy(key, path, number);
    }

    @Override
    public RedisFuture<Long> strAppend(K key, V json) {
        return delegate.strAppend(key, json);
    }

    @Override
    public RedisFuture<Long> strAppend(K key, K path, V json) {
        return delegate.strAppend(key, path, json);
    }

    @Override
    public RedisFuture<Long> strLen(K key) {
        return delegate.strLen(key);
    }

    @Override
    public RedisFuture<Long> strLen(K key, K path) {
        return delegate.strLen(key, path);
    }

    @Override
    public RedisFuture<Long> arrAppend(K key, K path, V... jsons) {
        return delegate.arrAppend(key, path, jsons);
    }

    @Override
    public RedisFuture<Long> arrIndex(K key, K path, V scalar) {
        return delegate.arrIndex(key, path, scalar);
    }

    @Override
    public RedisFuture<Long> arrIndex(K key, K path, V scalar, long start) {
        return delegate.arrIndex(key, path, scalar, start);
    }

    @Override
    public RedisFuture<Long> arrIndex(K key, K path, V scalar, long start, long stop) {
        return delegate.arrIndex(key, path, scalar, start, stop);
    }

    @Override
    public RedisFuture<Long> arrInsert(K key, K path, long index, V... jsons) {
        return delegate.arrInsert(key, path, index, jsons);
    }

    @Override
    public RedisFuture<Long> arrLen(K key) {
        return delegate.arrLen(key);
    }

    @Override
    public RedisFuture<Long> arrLen(K key, K path) {
        return delegate.arrLen(key, path);
    }

    @Override
    public RedisFuture<V> arrPop(K key) {
        return delegate.arrPop(key);
    }

    @Override
    public RedisFuture<V> arrPop(K key, K path) {
        return delegate.arrPop(key, path);
    }

    @Override
    public RedisFuture<V> arrPop(K key, K path, long index) {
        return delegate.arrPop(key, path, index);
    }

    @Override
    public RedisFuture<Long> arrTrim(K key, K path, long start, long stop) {
        return delegate.arrTrim(key, path, start, stop);
    }

    @Override
    public RedisFuture<List<K>> objKeys(K key) {
        return delegate.objKeys(key);
    }

    @Override
    public RedisFuture<List<K>> objKeys(K key, K path) {
        return delegate.objKeys(key, path);
    }

    @Override
    public RedisFuture<Long> objLen(K key) {
        return delegate.objLen(key);
    }

    @Override
    public RedisFuture<Long> objLen(K key, K path) {
        return delegate.objLen(key, path);
    }
}
