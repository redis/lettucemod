package com.redis.lettucemod;

import com.redis.lettucemod.api.JsonGetOptions;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.api.gears.Execution;
import com.redis.lettucemod.api.gears.ExecutionDetails;
import com.redis.lettucemod.api.gears.ExecutionMode;
import com.redis.lettucemod.api.gears.Registration;
import com.redis.lettucemod.api.search.AggregateOptions;
import com.redis.lettucemod.api.search.AggregateResults;
import com.redis.lettucemod.api.search.AggregateWithCursorResults;
import com.redis.lettucemod.api.search.Cursor;
import com.redis.lettucemod.api.search.Field;
import com.redis.lettucemod.api.search.SearchOptions;
import com.redis.lettucemod.api.search.SearchResults;
import com.redis.lettucemod.api.search.SugaddOptions;
import com.redis.lettucemod.api.search.Suggestion;
import com.redis.lettucemod.api.search.SuggetOptions;
import com.redis.lettucemod.api.timeseries.Aggregation;
import com.redis.lettucemod.api.timeseries.CreateOptions;
import com.redis.lettucemod.api.timeseries.GetResult;
import com.redis.lettucemod.api.timeseries.KeySample;
import com.redis.lettucemod.api.timeseries.RangeOptions;
import com.redis.lettucemod.api.timeseries.RangeResult;
import com.redis.lettucemod.api.timeseries.Sample;
import com.redis.lettucemod.output.ExecutionResults;
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
    public RedisFuture<String> abortexecution(String id) {
        return dispatch(gearsCommandBuilder.abortExecution(id));
    }

    @Override
    public RedisFuture<List<V>> configget(K... keys) {
        return dispatch(gearsCommandBuilder.configGet(keys));
    }

    @Override
    public RedisFuture<List<V>> configset(Map<K, V> map) {
        return dispatch(gearsCommandBuilder.configSet(map));
    }

    @Override
    public RedisFuture<String> dropexecution(String id) {
        return dispatch(gearsCommandBuilder.dropExecution(id));
    }

    @Override
    public RedisFuture<List<Execution>> dumpexecutions() {
        return dispatch(gearsCommandBuilder.dumpExecutions());
    }

    @Override
    public RedisFuture<ExecutionResults> pyexecute(String function, V... requirements) {
        return dispatch(gearsCommandBuilder.pyExecute(function, requirements));
    }

    @Override
    public RedisFuture<String> pyexecuteUnblocking(String function, V... requirements) {
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
    public RedisFuture<List<Registration>> dumpregistrations() {
        return dispatch(gearsCommandBuilder.dumpRegistrations());
    }

    @Override
    public RedisFuture<ExecutionDetails> getexecution(String id) {
        return dispatch(gearsCommandBuilder.getExecution(id));
    }

    @Override
    public RedisFuture<ExecutionDetails> getexecution(String id, ExecutionMode mode) {
        return dispatch(gearsCommandBuilder.getExecution(id, mode));
    }

    @Override
    public RedisFuture<ExecutionResults> getresults(String id) {
        return dispatch(gearsCommandBuilder.getResults(id));
    }

    @Override
    public RedisFuture<ExecutionResults> getresultsBlocking(String id) {
        return dispatch(gearsCommandBuilder.getResultsBlocking(id));
    }

    @Override
    public RedisFuture<String> create(K key, CreateOptions<K, V> options) {
        return dispatch(timeSeriesCommandBuilder.create(key, options));
    }

    @Override
    public RedisFuture<String> alter(K key, CreateOptions<K, V> options) {
        return dispatch(timeSeriesCommandBuilder.alter(key, options));
    }

    @Override
    public RedisFuture<Long> add(K key, long timestamp, double value) {
        return add(key, timestamp, value, null);
    }

    @Override
    public RedisFuture<Long> add(K key, long timestamp, double value, CreateOptions<K,V> options) {
        return dispatch(timeSeriesCommandBuilder.add(key, timestamp, value, options));
    }

    @Override
    public RedisFuture<List<Long>> madd(KeySample<K>... samples) {
        return dispatch(timeSeriesCommandBuilder.madd(samples));
    }

    @Override
    public RedisFuture<Long> incrby(K key, double value, Long timestamp, CreateOptions<K,V> options) {
        return dispatch(timeSeriesCommandBuilder.incrby(key, value, timestamp, options));
    }

    @Override
    public RedisFuture<Long> decrby(K key, double value, Long timestamp, CreateOptions<K,V> options) {
        return dispatch(timeSeriesCommandBuilder.decrby(key, value, timestamp, options));
    }

    @Override
    public RedisFuture<String> createrule(K sourceKey, K destKey, Aggregation aggregation) {
        return dispatch(timeSeriesCommandBuilder.createRule(sourceKey, destKey, aggregation));
    }

    @Override
    public RedisFuture<String> deleterule(K sourceKey, K destKey) {
        return dispatch(timeSeriesCommandBuilder.deleteRule(sourceKey, destKey));
    }

    @Override
    public RedisFuture<List<Sample>> range(K key, RangeOptions options) {
        return dispatch(timeSeriesCommandBuilder.range(key, options));
    }

    @Override
    public RedisFuture<List<Sample>> revrange(K key, RangeOptions options) {
        return dispatch(timeSeriesCommandBuilder.revrange(key, options));
    }

    @Override
    public RedisFuture<List<RangeResult<K, V>>> mrange(RangeOptions options, V... filters) {
        return dispatch(timeSeriesCommandBuilder.mrange(options, filters));
    }

    @Override
    public RedisFuture<List<RangeResult<K, V>>> mrangeWithLabels(RangeOptions options, V... filters) {
        return dispatch(timeSeriesCommandBuilder.mrangeWithLabels(options, filters));
    }

    @Override
    public RedisFuture<List<RangeResult<K, V>>> mrevrange(RangeOptions options, V... filters) {
        return dispatch(timeSeriesCommandBuilder.mrevrange(options, filters));
    }


    @Override
    public RedisFuture<List<RangeResult<K, V>>> mrevrangeWithLabels(RangeOptions options, V... filters) {
        return dispatch(timeSeriesCommandBuilder.mrevrangeWithLabels(options, filters));
    }

    @Override
    public RedisFuture<Sample> tsGet(K key) {
        return dispatch(timeSeriesCommandBuilder.get(key));
    }

    @Override
    public RedisFuture<List<GetResult<K, V>>> tsMget(V... filters) {
        return dispatch(timeSeriesCommandBuilder.mget(false, filters));
    }

    @Override
    public RedisFuture<List<GetResult<K, V>>> tsMgetWithLabels(V... filters) {
        return dispatch(timeSeriesCommandBuilder.mget(true, filters));
    }

    @Override
    public RedisFuture<List<Object>> tsInfo(K key) {
        return dispatch(timeSeriesCommandBuilder.info(key, false));
    }

    @Override
    public RedisFuture<List<Object>> tsInfoDebug(K key) {
        return dispatch(timeSeriesCommandBuilder.info(key, true));
    }

    @Override
    public RedisFuture<String> create(K index, Field... fields) {
        return create(index, null, fields);
    }

    @Override
    public RedisFuture<String> create(K index, com.redis.lettucemod.api.search.CreateOptions<K, V> options, Field... fields) {
        return dispatch(searchCommandBuilder.create(index, options, fields));
    }

    @Override
    public RedisFuture<String> dropindex(K index) {
        return dispatch(searchCommandBuilder.dropIndex(index, false));
    }

    @Override
    public RedisFuture<String> dropindexDeleteDocs(K index) {
        return dispatch(searchCommandBuilder.dropIndex(index, true));
    }

    @Override
    public RedisFuture<List<Object>> indexInfo(K index) {
        return dispatch(searchCommandBuilder.info(index));
    }

    @Override
    public RedisFuture<SearchResults<K, V>> search(K index, V query) {
        return search(index, query, null);
    }

    @Override
    public RedisFuture<SearchResults<K, V>> search(K index, V query, SearchOptions<K, V> options) {
        return dispatch(searchCommandBuilder.search(index, query, options));
    }

    @Override
    public RedisFuture<AggregateResults<K>> aggregate(K index, V query) {
        return aggregate(index, query, (AggregateOptions<K, V>) null);
    }

    @Override
    public RedisFuture<AggregateResults<K>> aggregate(K index, V query, AggregateOptions<K, V> options) {
        return dispatch(searchCommandBuilder.aggregate(index, query, options));
    }

    @Override
    public RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor) {
        return aggregate(index, query, cursor, null);
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
    public RedisFuture<String> aliasadd(K name, K index) {
        return dispatch(searchCommandBuilder.aliasAdd(name, index));
    }

    @Override
    public RedisFuture<String> aliasdel(K name) {
        return dispatch(searchCommandBuilder.aliasDel(name));
    }

    @Override
    public RedisFuture<String> aliasupdate(K name, K index) {
        return dispatch(searchCommandBuilder.aliasUpdate(name, index));
    }

    @Override
    public RedisFuture<List<K>> list() {
        return dispatch(searchCommandBuilder.list());
    }

    @Override
    public RedisFuture<List<V>> tagvals(K index, K field) {
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
    public RedisFuture<Long> jsonDel(K key, K path) {
        return dispatch(jsonCommandBuilder.del(key, path));
    }

    @Override
    public RedisFuture<V> get(K key, JsonGetOptions<K, V> options, K... paths) {
        return dispatch(jsonCommandBuilder.get(key, options, paths));
    }

    @Override
    public RedisFuture<List<KeyValue<K, V>>> jsonMget(K path, K... keys) {
        return dispatch(jsonCommandBuilder.mgetKeyValue(path, keys));
    }

    public RedisFuture<List<KeyValue<K, V>>> mget(K path, Iterable<K> keys) {
        return dispatch(jsonCommandBuilder.mgetKeyValue(path, keys));
    }

    @Override
    public RedisFuture<Long> jsonMget(KeyValueStreamingChannel<K, V> channel, K path, K... keys) {
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
    public RedisFuture<V> numincrby(K key, K path, double number) {
        return dispatch(jsonCommandBuilder.numIncrBy(key, path, number));
    }

    @Override
    public RedisFuture<V> nummultby(K key, K path, double number) {
        return dispatch(jsonCommandBuilder.numMultBy(key, path, number));
    }

    @Override
    public RedisFuture<Long> strappend(K key, V json) {
        return strappend(key, null, json);
    }

    @Override
    public RedisFuture<Long> strappend(K key, K path, V json) {
        return dispatch(jsonCommandBuilder.strAppend(key, path, json));
    }

    @Override
    public RedisFuture<Long> strlen(K key) {
        return strlen(key, null);
    }

    @Override
    public RedisFuture<Long> strlen(K key, K path) {
        return dispatch(jsonCommandBuilder.strLen(key, path));
    }

    @Override
    public RedisFuture<Long> arrappend(K key, K path, V... jsons) {
        return dispatch(jsonCommandBuilder.arrAppend(key, path, jsons));
    }

    @Override
    public RedisFuture<Long> arrindex(K key, K path, V scalar) {
        return dispatch(jsonCommandBuilder.arrIndex(key, path, scalar, null, null));
    }

    @Override
    public RedisFuture<Long> arrindex(K key, K path, V scalar, long start) {
        return dispatch(jsonCommandBuilder.arrIndex(key, path, scalar, start, null));
    }

    @Override
    public RedisFuture<Long> arrindex(K key, K path, V scalar, long start, long stop) {
        return dispatch(jsonCommandBuilder.arrIndex(key, path, scalar, start, stop));
    }

    @Override
    public RedisFuture<Long> arrinsert(K key, K path, long index, V... jsons) {
        return dispatch(jsonCommandBuilder.arrInsert(key, path, index, jsons));
    }

    @Override
    public RedisFuture<Long> arrlen(K key) {
        return arrlen(key, null);
    }

    @Override
    public RedisFuture<Long> arrlen(K key, K path) {
        return dispatch(jsonCommandBuilder.arrLen(key, path));
    }

    @Override
    public RedisFuture<V> arrpop(K key) {
        return arrpop(key, null);
    }

    @Override
    public RedisFuture<V> arrpop(K key, K path) {
        return dispatch(jsonCommandBuilder.arrPop(key, path, null));
    }

    @Override
    public RedisFuture<V> arrpop(K key, K path, long index) {
        return dispatch(jsonCommandBuilder.arrPop(key, path, index));
    }

    @Override
    public RedisFuture<Long> arrtrim(K key, K path, long start, long stop) {
        return dispatch(jsonCommandBuilder.arrTrim(key, path, start, stop));
    }

    @Override
    public RedisFuture<List<K>> objkeys(K key) {
        return objkeys(key, null);
    }

    @Override
    public RedisFuture<List<K>> objkeys(K key, K path) {
        return dispatch(jsonCommandBuilder.objKeys(key, path));
    }

    @Override
    public RedisFuture<Long> objlen(K key) {
        return objlen(key, null);
    }

    @Override
    public RedisFuture<Long> objlen(K key, K path) {
        return dispatch(jsonCommandBuilder.objLen(key, path));
    }


}
