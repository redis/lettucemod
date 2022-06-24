package com.redis.lettucemod.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.redis.lettucemod.RedisModulesAsyncCommandsImpl;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.lettucemod.cluster.api.async.RedisModulesAdvancedClusterAsyncCommands;
import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.ExecutionMode;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.SetMode;
import com.redis.lettucemod.output.ExecutionResults;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateResults;
import com.redis.lettucemod.search.AggregateWithCursorResults;
import com.redis.lettucemod.search.CursorOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.CreateRuleOptions;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.KeySample;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.cluster.MultiNodeExecution;
import io.lettuce.core.cluster.PipelinedRedisFuture;
import io.lettuce.core.cluster.RedisAdvancedClusterAsyncCommandsImpl;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.KeyValueStreamingChannel;

@SuppressWarnings("unchecked")
public class RedisModulesAdvancedClusterAsyncCommandsImpl<K, V> extends RedisAdvancedClusterAsyncCommandsImpl<K, V>
		implements RedisModulesAdvancedClusterAsyncCommands<K, V> {

	private final RedisModulesAsyncCommandsImpl<K, V> delegate;
	private final RedisCodec<K, V> codec;

	public RedisModulesAdvancedClusterAsyncCommandsImpl(StatefulRedisModulesClusterConnection<K, V> connection,
			RedisCodec<K, V> codec) {
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
	public RedisFuture<String> abortexecution(String id) {
		return delegate.abortexecution(id);
	}

	@Override
	public RedisFuture<List<V>> configget(K... keys) {
		return delegate.configget(keys);
	}

	@Override
	public RedisFuture<List<V>> configset(Map<K, V> map) {
		return delegate.configset(map);
	}

	@Override
	public RedisFuture<String> dropexecution(String id) {
		return delegate.dropexecution(id);
	}

	@Override
	public RedisFuture<List<Execution>> dumpexecutions() {
		return delegate.dumpexecutions();
	}

	@Override
	public RedisFuture<List<Registration>> dumpregistrations() {
		return delegate.dumpregistrations();
	}

	@Override
	public RedisFuture<ExecutionDetails> getexecution(String id) {
		return delegate.getexecution(id);
	}

	@Override
	public RedisFuture<ExecutionDetails> getexecution(String id, ExecutionMode mode) {
		return delegate.getexecution(id, mode);
	}

	@Override
	public RedisFuture<ExecutionResults> getresults(String id) {
		return delegate.getresults(id);
	}

	@Override
	public RedisFuture<ExecutionResults> getresultsBlocking(String id) {
		return delegate.getresultsBlocking(id);
	}

	@Override
	public RedisFuture<ExecutionResults> pyexecute(String function, V... requirements) {
		return delegate.pyexecute(function, requirements);
	}

	@Override
	public RedisFuture<String> pyexecuteUnblocking(String function, V... requirements) {
		return delegate.pyexecuteUnblocking(function, requirements);
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
	public RedisFuture<String> create(K index, com.redis.lettucemod.search.CreateOptions<K, V> options,
			Field... fields) {
		return MultiNodeExecution.firstOfAsync(executeOnUpstream(
				commands -> ((RedisModulesAsyncCommands<K, V>) commands).create(index, options, fields)));
	}

	@Override
	public RedisFuture<String> dropindex(K index) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).dropindex(index)));
	}

	@Override
	public RedisFuture<String> dropindexDeleteDocs(K index) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).dropindexDeleteDocs(index)));
	}

	@Override
	public RedisFuture<String> alter(K index, Field field) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).alter(index, field)));
	}

	@Override
	public RedisFuture<List<Object>> indexInfo(K index) {
		return delegate.indexInfo(index);
	}

	@Override
	public RedisFuture<String> aliasadd(K name, K index) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).aliasadd(name, index)));
	}

	@Override
	public RedisFuture<String> aliasupdate(K name, K index) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).aliasupdate(name, index)));
	}

	@Override
	public RedisFuture<String> aliasdel(K name) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).aliasdel(name)));
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
	public RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, CursorOptions cursor) {
		return delegate.aggregate(index, query, cursor);
	}

	@Override
	public RedisFuture<AggregateWithCursorResults<K>> aggregate(K index, V query, CursorOptions cursor,
			AggregateOptions<K, V> options) {
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
	public RedisFuture<List<V>> tagvals(K index, K field) {
		return delegate.tagvals(index, field);
	}

	@Override
	public RedisFuture<Long> sugadd(K key, V string, double score) {
		return delegate.sugadd(key, string, score);
	}

	@Override
	public RedisFuture<Long> sugaddIncr(K key, V string, double score) {
		return delegate.sugaddIncr(key, string, score);
	}

	@Override
	public RedisFuture<Long> sugadd(K key, V string, double score, V payload) {
		return delegate.sugadd(key, string, score, payload);
	}

	@Override
	public RedisFuture<Long> sugaddIncr(K key, V string, double score, V payload) {
		return delegate.sugaddIncr(key, string, score, payload);
	}

	@Override
	public RedisFuture<Long> sugadd(K key, Suggestion<V> suggestion) {
		return delegate.sugadd(key, suggestion);
	}

	@Override
	public RedisFuture<Long> sugaddIncr(K key, Suggestion<V> suggestion) {
		return delegate.sugaddIncr(key, suggestion);
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
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).dictadd(dict, terms)));
	}

	@Override
	public RedisFuture<Long> dictdel(K dict, V... terms) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).dictdel(dict, terms)));
	}

	@Override
	public RedisFuture<List<V>> dictdump(K dict) {
		return delegate.dictdump(dict);
	}

	@Override
	public RedisFuture<String> create(K key, CreateOptions<K, V> options) {
		return delegate.create(key, options);
	}

	@Override
	public RedisFuture<String> alter(K key, CreateOptions<K, V> options) {
		return delegate.alter(key, options);
	}

	@Override
	public RedisFuture<Long> add(K key, long timestamp, double value) {
		return delegate.add(key, timestamp, value);
	}

	@Override
	public RedisFuture<Long> add(K key, long timestamp, double value, AddOptions<K, V> options) {
		return delegate.add(key, timestamp, value, options);
	}

	@Override
	public RedisFuture<Long> addAutoTimestamp(K key, double value) {
		return delegate.addAutoTimestamp(key, value);
	}

	@Override
	public RedisFuture<Long> addAutoTimestamp(K key, double value, AddOptions<K, V> options) {
		return delegate.addAutoTimestamp(key, value, options);
	}

	@Override
	public RedisFuture<Long> add(K key, Sample sample) {
		return delegate.add(key, sample);
	}

	@Override
	public RedisFuture<Long> add(K key, Sample sample, AddOptions<K, V> options) {
		return delegate.add(key, sample, options);
	}

	@Override
	public RedisFuture<List<Long>> madd(KeySample<K>... samples) {
		return delegate.madd(samples);
	}

	@Override
	public RedisFuture<Long> incrby(K key, double value, Long timestamp, CreateOptions<K, V> options) {
		return delegate.incrby(key, value, timestamp, options);
	}

	@Override
	public RedisFuture<Long> decrby(K key, double value, Long timestamp, CreateOptions<K, V> options) {
		return delegate.decrby(key, value, timestamp, options);
	}

	@Override
	public RedisFuture<Long> incrbyAutoTimestamp(K key, double value, CreateOptions<K, V> options) {
		return delegate.incrbyAutoTimestamp(key, value, options);
	}

	@Override
	public RedisFuture<Long> decrbyAutoTimestamp(K key, double value, CreateOptions<K, V> options) {
		return delegate.decrbyAutoTimestamp(key, value, options);
	}

	@Override
	public RedisFuture<String> createrule(K sourceKey, K destKey, CreateRuleOptions options) {
		return delegate.createrule(sourceKey, destKey, options);
	}

	@Override
	public RedisFuture<String> deleterule(K sourceKey, K destKey) {
		return delegate.deleterule(sourceKey, destKey);
	}

	@Override
	public RedisFuture<List<Sample>> range(K key, TimeRange range, RangeOptions options) {
		return delegate.range(key, range, options);
	}

	@Override
	public RedisFuture<List<Sample>> revrange(K key, TimeRange range, RangeOptions options) {
		return delegate.revrange(key, range, options);
	}

	@Override
	public RedisFuture<List<RangeResult<K, V>>> mrange(TimeRange range, MRangeOptions<K, V> options) {
		return delegate.mrange(range, options);
	}

	@Override
	public RedisFuture<List<RangeResult<K, V>>> mrevrange(TimeRange range, MRangeOptions<K, V> options) {
		return delegate.mrevrange(range, options);
	}

	@Override
	public RedisFuture<Sample> tsGet(K key) {
		return delegate.tsGet(key);
	}

	@Override
	public RedisFuture<List<GetResult<K, V>>> tsMget(V... filters) {
		return delegate.tsMget(filters);
	}

	@Override
	public RedisFuture<List<GetResult<K, V>>> tsMgetWithLabels(V... filters) {
		return delegate.tsMgetWithLabels(filters);
	}

	@Override
	public RedisFuture<List<Object>> tsInfo(K key) {
		return delegate.tsInfo(key);
	}

	@Override
	public RedisFuture<List<Object>> tsInfoDebug(K key) {
		return delegate.tsInfoDebug(key);
	}

	@Override
	public RedisFuture<Long> jsonDel(K key) {
		return delegate.jsonDel(key);
	}

	@Override
	public RedisFuture<Long> jsonDel(K key, K path) {
		return delegate.jsonDel(key, path);
	}

	@Override
	public RedisFuture<V> jsonGet(K key, K... paths) {
		return delegate.jsonGet(key, paths);
	}

	@Override
	public RedisFuture<V> jsonGet(K key, GetOptions options, K... paths) {
		return delegate.jsonGet(key, options, paths);
	}

	@Override
	public RedisFuture<List<KeyValue<K, V>>> jsonMget(K path, K... keys) {
		return mget(path, Arrays.asList(keys));
	}

	@Override
	public RedisFuture<Long> jsonMget(KeyValueStreamingChannel<K, V> channel, K path, K... keys) {
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
	public RedisFuture<String> jsonSet(K key, K path, V json) {
		return delegate.jsonSet(key, path, json);
	}

	@Override
	public RedisFuture<String> jsonSet(K key, K path, V json, SetMode mode) {
		return delegate.jsonSet(key, path, json, mode);
	}

	@Override
	public RedisFuture<String> jsonType(K key) {
		return delegate.jsonType(key);
	}

	@Override
	public RedisFuture<String> jsonType(K key, K path) {
		return delegate.jsonType(key, path);
	}

	@Override
	public RedisFuture<V> numincrby(K key, K path, double number) {
		return delegate.numincrby(key, path, number);
	}

	@Override
	public RedisFuture<V> nummultby(K key, K path, double number) {
		return delegate.nummultby(key, path, number);
	}

	@Override
	public RedisFuture<Long> strappend(K key, V json) {
		return delegate.strappend(key, json);
	}

	@Override
	public RedisFuture<Long> strappend(K key, K path, V json) {
		return delegate.strappend(key, path, json);
	}

	@Override
	public RedisFuture<Long> strlen(K key, K path) {
		return delegate.strlen(key, path);
	}

	@Override
	public RedisFuture<Long> arrappend(K key, K path, V... jsons) {
		return delegate.arrappend(key, path, jsons);
	}

	@Override
	public RedisFuture<Long> arrindex(K key, K path, V scalar) {
		return delegate.arrindex(key, path, scalar);
	}

	@Override
	public RedisFuture<Long> arrindex(K key, K path, V scalar, long start) {
		return delegate.arrindex(key, path, scalar, start);
	}

	@Override
	public RedisFuture<Long> arrindex(K key, K path, V scalar, long start, long stop) {
		return delegate.arrindex(key, path, scalar, start, stop);
	}

	@Override
	public RedisFuture<Long> arrinsert(K key, K path, long index, V... jsons) {
		return delegate.arrinsert(key, path, index, jsons);
	}

	@Override
	public RedisFuture<Long> arrlen(K key) {
		return delegate.arrlen(key);
	}

	@Override
	public RedisFuture<Long> arrlen(K key, K path) {
		return delegate.arrlen(key, path);
	}

	@Override
	public RedisFuture<V> arrpop(K key) {
		return delegate.arrpop(key);
	}

	@Override
	public RedisFuture<V> arrpop(K key, K path) {
		return delegate.arrpop(key, path);
	}

	@Override
	public RedisFuture<V> arrpop(K key, K path, long index) {
		return delegate.arrpop(key, path, index);
	}

	@Override
	public RedisFuture<Long> arrtrim(K key, K path, long start, long stop) {
		return delegate.arrtrim(key, path, start, stop);
	}

	@Override
	public RedisFuture<List<K>> objkeys(K key) {
		return delegate.objkeys(key);
	}

	@Override
	public RedisFuture<List<K>> objkeys(K key, K path) {
		return delegate.objkeys(key, path);
	}

	@Override
	public RedisFuture<Long> objlen(K key) {
		return delegate.objlen(key);
	}

	@Override
	public RedisFuture<Long> objlen(K key, K path) {
		return delegate.objlen(key, path);
	}

}
