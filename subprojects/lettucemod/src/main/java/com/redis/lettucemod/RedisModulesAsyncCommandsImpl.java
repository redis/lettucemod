package com.redis.lettucemod;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.ExecutionMode;
import com.redis.lettucemod.gears.RedisGearsCommandBuilder;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.gears.output.ExecutionResults;
import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.RedisJSONCommandBuilder;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateResults;
import com.redis.lettucemod.search.AggregateWithCursorResults;
import com.redis.lettucemod.search.Cursor;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.RediSearchCommandBuilder;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.SugaddOptions;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.RedisTimeSeriesCommandBuilder;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisAsyncCommandsImpl;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.KeyValueStreamingChannel;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class RedisModulesAsyncCommandsImpl<K, V> extends RedisAsyncCommandsImpl<K, V> implements RedisModulesAsyncCommands<K, V> {

    private final RedisGearsCommandBuilder<K, V> gearsCommandBuilder;
    private final RedisTimeSeriesCommandBuilder<K, V> timeSeriesCommandBuilder;
    private final RediSearchCommandBuilder<K, V> searchCommandBuilder;
    private final RedisJSONCommandBuilder<K, V> jsonCommandBuilder;

    public RedisModulesAsyncCommandsImpl(StatefulRedisModulesConnection<K, V> connection, RedisCodec<K, V> codec) {
        super(connection, codec);
        this.gearsCommandBuilder = new RedisGearsCommandBuilder<>(codec);
        this.timeSeriesCommandBuilder = new RedisTimeSeriesCommandBuilder<>(codec);
        this.searchCommandBuilder = new RediSearchCommandBuilder<>(codec);
        this.jsonCommandBuilder = new RedisJSONCommandBuilder<>(codec);
    }

    @Override
    public StatefulRedisModulesConnection<K, V> getStatefulConnection() {
        return (StatefulRedisModulesConnection<K, V>) super.getStatefulConnection();
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


    @Override
    public RedisFuture<String> create(K index, Field... fields) {
        return create(index, null, fields);
    }

    @Override
    public RedisFuture<String> create(K index, com.redis.lettucemod.search.CreateOptions<K, V> options, Field... fields) {
        return dispatch(searchCommandBuilder.create(index, options, fields));
    }

    @Override
    public RedisFuture<String> dropIndex(K index) {
        return dispatch(searchCommandBuilder.dropIndex(index, false));
    }

    @Override
    public RedisFuture<String> dropIndexDeleteDocs(K index) {
        return dispatch(searchCommandBuilder.dropIndex(index, true));
    }

    @Override
    public RedisFuture<List<Object>> indexInfo(K index) {
        return dispatch(searchCommandBuilder.info(index));
    }

    @Override
    public RedisFuture<SearchResults<K, V>> search(K index, V query) {
        return dispatch(searchCommandBuilder.search(index, query, null));
    }

    @Override
    public RedisFuture<SearchResults<K, V>> search(K index, V query, SearchOptions<K, V> options) {
        return dispatch(searchCommandBuilder.search(index, query, options));
    }

    @Override
    public RedisFuture<AggregateResults<K>> aggregate(K index, V query) {
        return dispatch(searchCommandBuilder.aggregate(index, query, null));
    }

    @Override
    public RedisFuture<AggregateResults<K>> aggregate(K index, V query, AggregateOptions<K, V> options) {
        return dispatch(searchCommandBuilder.aggregate(index, query, options));
    }

    @Override
    public RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor) {
        return dispatch(searchCommandBuilder.aggregate(index, query, cursor, null));
    }

    @Override
    public RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor, AggregateOptions<K, V> options) {
        return dispatch(searchCommandBuilder.aggregate(index, query, cursor, options));
    }

    @Override
    public RedisFuture<AggregateWithCursorResults<K>> cursorRead(K index, long cursor) {
        return dispatch(searchCommandBuilder.cursorRead(index, cursor, null));
    }

    @Override
    public RedisFuture<AggregateWithCursorResults<K>> cursorRead(K index, long cursor, long count) {
        return dispatch(searchCommandBuilder.cursorRead(index, cursor, count));
    }

    @Override
    public RedisFuture<String> cursorDelete(K index, long cursor) {
        return dispatch(searchCommandBuilder.cursorDelete(index, cursor));
    }

    @Override
    public RedisFuture<Long> sugadd(K key, V string, double score) {
        return dispatch(searchCommandBuilder.sugadd(key, string, score));
    }

    @Override
    public RedisFuture<Long> sugadd(K key, V string, double score, SugaddOptions<K, V> options) {
        return dispatch(searchCommandBuilder.sugadd(key, string, score, options));
    }

    @Override
    public RedisFuture<List<Suggestion<V>>> sugget(K key, V prefix) {
        return dispatch(searchCommandBuilder.sugget(key, prefix));
    }

    @Override
    public RedisFuture<List<Suggestion<V>>> sugget(K key, V prefix, SuggetOptions options) {
        return dispatch(searchCommandBuilder.sugget(key, prefix, options));
    }

    @Override
    public RedisFuture<Boolean> sugdel(K key, V string) {
        return dispatch(searchCommandBuilder.sugdel(key, string));
    }

    @Override
    public RedisFuture<Long> suglen(K key) {
        return dispatch(searchCommandBuilder.suglen(key));
    }

    @Override
    public RedisFuture<String> alter(K index, Field field) {
        return dispatch(searchCommandBuilder.alter(index, field));
    }

    @Override
    public RedisFuture<String> aliasAdd(K name, K index) {
        return dispatch(searchCommandBuilder.aliasAdd(name, index));
    }

    @Override
    public RedisFuture<String> aliasDel(K name) {
        return dispatch(searchCommandBuilder.aliasDel(name));
    }

    @Override
    public RedisFuture<String> aliasUpdate(K name, K index) {
        return dispatch(searchCommandBuilder.aliasUpdate(name, index));
    }

    @Override
    public RedisFuture<List<K>> list() {
        return dispatch(searchCommandBuilder.list());
    }

    @Override
    public RedisFuture<List<V>> tagVals(K index, K field) {
        return dispatch(searchCommandBuilder.tagVals(index, field));
    }

    @Override
    public RedisFuture<Long> dictadd(K dict, V... terms) {
        return dispatch(searchCommandBuilder.dictadd(dict, terms));
    }

    @Override
    public RedisFuture<Long> dictdel(K dict, V... terms) {
        return dispatch(searchCommandBuilder.dictdel(dict, terms));
    }

    @Override
    public RedisFuture<List<V>> dictdump(K dict) {
        return dispatch(searchCommandBuilder.dictdump(dict));
    }

    @Override
    public RedisFuture<Long> del(K key) {
        return del(key, null);
    }

    @Override
    public RedisFuture<Long> del(K key, K path) {
        return dispatch(jsonCommandBuilder.del(key, path));
    }

    @Override
    public RedisFuture<V> get(K key, K... paths) {
        return dispatch(jsonCommandBuilder.get(key, null, paths));
    }

    @Override
    public RedisFuture<V> get(K key, GetOptions<K, V> options, K... paths) {
        return dispatch(jsonCommandBuilder.get(key, options, paths));
    }

    @Override
    public RedisFuture<List<KeyValue<K, V>>> mget(K path, K... keys) {
        return dispatch(jsonCommandBuilder.mgetKeyValue(path, keys));
    }

    public RedisFuture<List<KeyValue<K, V>>> mget(K path, Iterable<K> keys) {
        return dispatch(jsonCommandBuilder.mgetKeyValue(path, keys));
    }

    @Override
    public RedisFuture<Long> mget(KeyValueStreamingChannel<K, V> channel, K path, K... keys) {
        return dispatch(jsonCommandBuilder.mget(channel, path, keys));
    }

    public RedisFuture<Long> mget(KeyValueStreamingChannel<K, V> channel, K path, Iterable<K> keys) {
        return dispatch(jsonCommandBuilder.mget(channel, path, keys));
    }

    @Override
    public RedisFuture<String> set(K key, K path, V json) {
        return dispatch(jsonCommandBuilder.set(key, path, json, null));
    }

    @Override
    public RedisFuture<String> setNX(K key, K path, V json) {
        return dispatch(jsonCommandBuilder.set(key, path, json, RedisJSONCommandBuilder.SetMode.NX));
    }

    @Override
    public RedisFuture<String> setXX(K key, K path, V json) {
        return dispatch(jsonCommandBuilder.set(key, path, json, RedisJSONCommandBuilder.SetMode.XX));
    }

    @Override
    public RedisFuture<String> type(K key) {
        return type(key, null);
    }

    @Override
    public RedisFuture<String> type(K key, K path) {
        return dispatch(jsonCommandBuilder.type(key, path));
    }

    @Override
    public RedisFuture<V> numIncrBy(K key, K path, double number) {
        return dispatch(jsonCommandBuilder.numIncrBy(key, path, number));
    }

    @Override
    public RedisFuture<V> numMultBy(K key, K path, double number) {
        return dispatch(jsonCommandBuilder.numMultBy(key, path, number));
    }

    @Override
    public RedisFuture<Long> strAppend(K key, V json) {
        return strAppend(key, null, json);
    }

    @Override
    public RedisFuture<Long> strAppend(K key, K path, V json) {
        return dispatch(jsonCommandBuilder.strAppend(key, path, json));
    }

    @Override
    public RedisFuture<Long> strLen(K key) {
        return strLen(key, null);
    }

    @Override
    public RedisFuture<Long> strLen(K key, K path) {
        return dispatch(jsonCommandBuilder.strLen(key, path));
    }

    @Override
    public RedisFuture<Long> arrAppend(K key, K path, V... jsons) {
        return dispatch(jsonCommandBuilder.arrAppend(key, path, jsons));
    }

    @Override
    public RedisFuture<Long> arrIndex(K key, K path, V scalar) {
        return dispatch(jsonCommandBuilder.arrIndex(key, path, scalar, null, null));
    }

    @Override
    public RedisFuture<Long> arrIndex(K key, K path, V scalar, long start) {
        return dispatch(jsonCommandBuilder.arrIndex(key, path, scalar, start, null));
    }

    @Override
    public RedisFuture<Long> arrIndex(K key, K path, V scalar, long start, long stop) {
        return dispatch(jsonCommandBuilder.arrIndex(key, path, scalar, start, stop));
    }

    @Override
    public RedisFuture<Long> arrInsert(K key, K path, long index, V... jsons) {
        return dispatch(jsonCommandBuilder.arrInsert(key, path, index, jsons));
    }

    @Override
    public RedisFuture<Long> arrLen(K key) {
        return arrLen(key, null);
    }

    @Override
    public RedisFuture<Long> arrLen(K key, K path) {
        return dispatch(jsonCommandBuilder.arrLen(key, path));
    }

    @Override
    public RedisFuture<V> arrPop(K key) {
        return arrPop(key, null);
    }

    @Override
    public RedisFuture<V> arrPop(K key, K path) {
        return dispatch(jsonCommandBuilder.arrPop(key, path, null));
    }

    @Override
    public RedisFuture<V> arrPop(K key, K path, long index) {
        return dispatch(jsonCommandBuilder.arrPop(key, path, index));
    }

    @Override
    public RedisFuture<Long> arrTrim(K key, K path, long start, long stop) {
        return dispatch(jsonCommandBuilder.arrTrim(key, path, start, stop));
    }

    @Override
    public RedisFuture<List<K>> objKeys(K key) {
        return objKeys(key, null);
    }

    @Override
    public RedisFuture<List<K>> objKeys(K key, K path) {
        return dispatch(jsonCommandBuilder.objKeys(key, path));
    }

    @Override
    public RedisFuture<Long> objLen(K key) {
        return objLen(key, null);
    }

    @Override
    public RedisFuture<Long> objLen(K key, K path) {
        return dispatch(jsonCommandBuilder.objLen(key, path));
    }


}
