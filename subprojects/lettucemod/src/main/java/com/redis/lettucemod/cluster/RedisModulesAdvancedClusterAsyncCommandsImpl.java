package com.redis.lettucemod.cluster;

import java.util.*;

import com.redis.lettucemod.RedisModulesAsyncCommandsImpl;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.bloom.*;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.lettucemod.cluster.api.async.RedisModulesAdvancedClusterAsyncCommands;
import com.redis.lettucemod.cms.CmsInfo;
import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.ExecutionMode;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.json.ArrpopOptions;
import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.SetMode;
import com.redis.lettucemod.json.Slice;
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
import com.redis.lettucemod.timeseries.AlterOptions;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.CreateRuleOptions;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.IncrbyOptions;
import com.redis.lettucemod.timeseries.KeySample;
import com.redis.lettucemod.timeseries.MGetOptions;
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
import io.lettuce.core.cluster.SlotHash;
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
	public RedisFuture<String> rgAbortexecution(String id) {
		return delegate.rgAbortexecution(id);
	}

	@Override
	public RedisFuture<List<V>> rgConfigget(K... keys) {
		return delegate.rgConfigget(keys);
	}

	@Override
	public RedisFuture<List<V>> rgConfigset(Map<K, V> map) {
		return delegate.rgConfigset(map);
	}

	@Override
	public RedisFuture<String> rgDropexecution(String id) {
		return delegate.rgDropexecution(id);
	}

	@Override
	public RedisFuture<List<Execution>> rgDumpexecutions() {
		return delegate.rgDumpexecutions();
	}

	@Override
	public RedisFuture<List<Registration>> rgDumpregistrations() {
		return delegate.rgDumpregistrations();
	}

	@Override
	public RedisFuture<ExecutionDetails> rgGetexecution(String id) {
		return delegate.rgGetexecution(id);
	}

	@Override
	public RedisFuture<ExecutionDetails> rgGetexecution(String id, ExecutionMode mode) {
		return delegate.rgGetexecution(id, mode);
	}

	@Override
	public RedisFuture<ExecutionResults> rgGetresults(String id) {
		return delegate.rgGetresults(id);
	}

	@Override
	public RedisFuture<ExecutionResults> rgGetresultsblocking(String id) {
		return delegate.rgGetresultsblocking(id);
	}

	@Override
	public RedisFuture<ExecutionResults> rgPyexecute(String function, V... requirements) {
		return delegate.rgPyexecute(function, requirements);
	}

	@Override
	public RedisFuture<String> rgPyexecuteUnblocking(String function, V... requirements) {
		return delegate.rgPyexecuteUnblocking(function, requirements);
	}

	@Override
	public RedisFuture<List<Object>> rgTrigger(String trigger, V... args) {
		return delegate.rgTrigger(trigger, args);
	}

	@Override
	public RedisFuture<String> rgUnregister(String id) {
		return delegate.rgUnregister(id);
	}

	@Override
	public RedisFuture<String> ftCreate(K index, Field<K>... fields) {
		return ftCreate(index, null, fields);
	}

	@Override
	public RedisFuture<String> ftCreate(K index, com.redis.lettucemod.search.CreateOptions<K, V> options,
			Field<K>... fields) {
		return MultiNodeExecution.firstOfAsync(executeOnUpstream(
				commands -> ((RedisModulesAsyncCommands<K, V>) commands).ftCreate(index, options, fields)));
	}

	@Override
	public RedisFuture<String> ftDropindex(K index) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).ftDropindex(index)));
	}

	@Override
	public RedisFuture<String> ftDropindexDeleteDocs(K index) {
		return MultiNodeExecution.firstOfAsync(executeOnUpstream(
				commands -> ((RedisModulesAsyncCommands<K, V>) commands).ftDropindexDeleteDocs(index)));
	}

	@Override
	public RedisFuture<String> ftAlter(K index, Field<K> field) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).ftAlter(index, field)));
	}

	@Override
	public RedisFuture<List<Object>> ftInfo(K index) {
		return delegate.ftInfo(index);
	}

	@Override
	public RedisFuture<String> ftAliasadd(K name, K index) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).ftAliasadd(name, index)));
	}

	@Override
	public RedisFuture<String> ftAliasupdate(K name, K index) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).ftAliasupdate(name, index)));
	}

	@Override
	public RedisFuture<String> ftAliasdel(K name) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).ftAliasdel(name)));
	}

	@Override
	public RedisFuture<List<K>> ftList() {
		return delegate.ftList();
	}

	@Override
	public RedisFuture<SearchResults<K, V>> ftSearch(K index, V query) {
		return delegate.ftSearch(index, query);
	}

	@Override
	public RedisFuture<SearchResults<K, V>> ftSearch(K index, V query, SearchOptions<K, V> options) {
		return delegate.ftSearch(index, query, options);
	}

	@Override
	public RedisFuture<AggregateResults<K>> ftAggregate(K index, V query) {
		return delegate.ftAggregate(index, query);
	}

	@Override
	public RedisFuture<AggregateResults<K>> ftAggregate(K index, V query, AggregateOptions<K, V> options) {
		return delegate.ftAggregate(index, query, options);
	}

	@Override
	public RedisFuture<AggregateWithCursorResults<K>> ftAggregate(K index, V query, CursorOptions cursor) {
		return delegate.ftAggregate(index, query, cursor);
	}

	@Override
	public RedisFuture<AggregateWithCursorResults<K>> ftAggregate(K index, V query, CursorOptions cursor,
			AggregateOptions<K, V> options) {
		return delegate.ftAggregate(index, query, cursor, options);
	}

	@Override
	public RedisFuture<AggregateWithCursorResults<K>> ftCursorRead(K index, long cursor) {
		return delegate.ftCursorRead(index, cursor);
	}

	@Override
	public RedisFuture<AggregateWithCursorResults<K>> ftCursorRead(K index, long cursor, long count) {
		return delegate.ftCursorRead(index, cursor, count);
	}

	@Override
	public RedisFuture<String> ftCursorDelete(K index, long cursor) {
		return delegate.ftCursorDelete(index, cursor);
	}

	@Override
	public RedisFuture<List<V>> ftTagvals(K index, K field) {
		return delegate.ftTagvals(index, field);
	}

	@Override
	public RedisFuture<Long> ftSugadd(K key, Suggestion<V> suggestion) {
		return delegate.ftSugadd(key, suggestion);
	}

	@Override
	public RedisFuture<Long> ftSugaddIncr(K key, Suggestion<V> suggestion) {
		return delegate.ftSugaddIncr(key, suggestion);
	}

	@Override
	public RedisFuture<List<Suggestion<V>>> ftSugget(K key, V prefix) {
		return delegate.ftSugget(key, prefix);
	}

	@Override
	public RedisFuture<List<Suggestion<V>>> ftSugget(K key, V prefix, SuggetOptions options) {
		return delegate.ftSugget(key, prefix, options);
	}

	@Override
	public RedisFuture<Boolean> ftSugdel(K key, V string) {
		return delegate.ftSugdel(key, string);
	}

	@Override
	public RedisFuture<Long> ftSuglen(K key) {
		return delegate.ftSuglen(key);
	}

	@Override
	public RedisFuture<Long> ftDictadd(K dict, V... terms) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).ftDictadd(dict, terms)));
	}

	@Override
	public RedisFuture<Long> ftDictdel(K dict, V... terms) {
		return MultiNodeExecution.firstOfAsync(
				executeOnUpstream(commands -> ((RedisModulesAsyncCommands<K, V>) commands).ftDictdel(dict, terms)));
	}

	@Override
	public RedisFuture<List<V>> ftDictdump(K dict) {
		return delegate.ftDictdump(dict);
	}

	@Override
	public RedisFuture<String> tsCreate(K key, CreateOptions<K, V> options) {
		return delegate.tsCreate(key, options);
	}

	@Override
	public RedisFuture<String> tsAlter(K key, AlterOptions<K, V> options) {
		return delegate.tsAlter(key, options);
	}

	@Override
	public RedisFuture<Long> tsAdd(K key, Sample sample) {
		return delegate.tsAdd(key, sample);
	}

	@Override
	public RedisFuture<Long> tsAdd(K key, Sample sample, AddOptions<K, V> options) {
		return delegate.tsAdd(key, sample, options);
	}

	@Override
	public RedisFuture<List<Long>> tsMadd(KeySample<K>... samples) {
		return delegate.tsMadd(samples);
	}

	@Override
	public RedisFuture<Long> tsDecrby(K key, double value) {
		return delegate.tsDecrby(key, value);
	}

	@Override
	public RedisFuture<Long> tsDecrby(K key, double value, IncrbyOptions<K, V> options) {
		return delegate.tsDecrby(key, value, options);
	}

	@Override
	public RedisFuture<Long> tsIncrby(K key, double value) {
		return delegate.tsIncrby(key, value);
	}

	@Override
	public RedisFuture<Long> tsIncrby(K key, double value, IncrbyOptions<K, V> options) {
		return delegate.tsIncrby(key, value, options);
	}

	@Override
	public RedisFuture<String> tsCreaterule(K sourceKey, K destKey, CreateRuleOptions options) {
		return delegate.tsCreaterule(sourceKey, destKey, options);
	}

	@Override
	public RedisFuture<String> tsDeleterule(K sourceKey, K destKey) {
		return delegate.tsDeleterule(sourceKey, destKey);
	}

	@Override
	public RedisFuture<List<Sample>> tsRange(K key, TimeRange range) {
		return delegate.tsRange(key, range);
	}

	@Override
	public RedisFuture<List<Sample>> tsRange(K key, TimeRange range, RangeOptions options) {
		return delegate.tsRange(key, range, options);
	}

	@Override
	public RedisFuture<List<Sample>> tsRevrange(K key, TimeRange range) {
		return delegate.tsRevrange(key, range);
	}

	@Override
	public RedisFuture<List<Sample>> tsRevrange(K key, TimeRange range, RangeOptions options) {
		return delegate.tsRevrange(key, range, options);
	}

	@Override
	public RedisFuture<List<RangeResult<K, V>>> tsMrange(TimeRange range) {
		return delegate.tsMrange(range);
	}

	@Override
	public RedisFuture<List<RangeResult<K, V>>> tsMrange(TimeRange range, MRangeOptions<K, V> options) {
		return delegate.tsMrange(range, options);
	}

	@Override
	public RedisFuture<List<RangeResult<K, V>>> tsMrevrange(TimeRange range) {
		return delegate.tsMrevrange(range);
	}

	@Override
	public RedisFuture<List<RangeResult<K, V>>> tsMrevrange(TimeRange range, MRangeOptions<K, V> options) {
		return delegate.tsMrevrange(range, options);
	}

	@Override
	public RedisFuture<Sample> tsGet(K key) {
		return delegate.tsGet(key);
	}

	@Override
	public RedisFuture<List<GetResult<K, V>>> tsMget(MGetOptions<K, V> options) {
		return delegate.tsMget(options);
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
	public RedisFuture<List<V>> tsQueryIndex(V... filters){return delegate.tsQueryIndex(filters);}

	@Override
	public RedisFuture<Long> tsDel(K key, TimeRange timeRange){return delegate.tsDel(key, timeRange);}

	@Override
	public RedisFuture<Long> jsonDel(K key) {
		return delegate.jsonDel(key);
	}

	@Override
	public RedisFuture<Long> jsonDel(K key, String path) {
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
	public RedisFuture<List<KeyValue<K, V>>> jsonMget(String path, K... keys) {
		return jsonMget(path, Arrays.asList(keys));
	}

	@Override
	public RedisFuture<Long> jsonMget(KeyValueStreamingChannel<K, V> channel, String path, K... keys) {
		return jsonMget(channel, path, Arrays.asList(keys));
	}

	public RedisFuture<List<KeyValue<K, V>>> jsonMget(String path, Iterable<K> keys) {
		Map<Integer, List<K>> partitioned = SlotHash.partition(codec, keys);

		if (partitioned.size() < 2) {
			return delegate.jsonMget(path, keys);
		}

		Map<K, Integer> slots = SlotHash.getSlots(partitioned);
		Map<Integer, RedisFuture<List<KeyValue<K, V>>>> executions = new HashMap<>();

		for (Map.Entry<Integer, List<K>> entry : partitioned.entrySet()) {
			RedisFuture<List<KeyValue<K, V>>> mget = delegate.jsonMget(path, entry.getValue());
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

	public RedisFuture<Long> jsonMget(KeyValueStreamingChannel<K, V> channel, String path, Iterable<K> keys) {
		Map<Integer, List<K>> partitioned = SlotHash.partition(codec, keys);

		if (partitioned.size() < 2) {
			return delegate.jsonMget(channel, path, keys);
		}

		Map<Integer, RedisFuture<Long>> executions = new HashMap<>();

		for (Map.Entry<Integer, List<K>> entry : partitioned.entrySet()) {
			RedisFuture<Long> del = delegate.jsonMget(channel, path, entry.getValue());
			executions.put(entry.getKey(), del);
		}

		return MultiNodeExecution.aggregateAsync(executions);
	}

	@Override
	public RedisFuture<String> jsonSet(K key, String path, V json) {
		return delegate.jsonSet(key, path, json);
	}

	@Override
	public RedisFuture<String> jsonSet(K key, String path, V json, SetMode mode) {
		return delegate.jsonSet(key, path, json, mode);
	}

	@Override
	public RedisFuture<String> jsonMerge(K key, String path, V json) {
		return delegate.jsonMerge(key, path, json);
	}

	@Override
	public RedisFuture<String> jsonType(K key) {
		return delegate.jsonType(key);
	}

	@Override
	public RedisFuture<String> jsonType(K key, String path) {
		return delegate.jsonType(key, path);
	}

	@Override
	public RedisFuture<V> jsonNumincrby(K key, String path, double number) {
		return delegate.jsonNumincrby(key, path, number);
	}

	@Override
	public RedisFuture<V> jsonNummultby(K key, String path, double number) {
		return delegate.jsonNummultby(key, path, number);
	}

	@Override
	public RedisFuture<Long> jsonStrappend(K key, V json) {
		return delegate.jsonStrappend(key, json);
	}

	@Override
	public RedisFuture<Long> jsonStrappend(K key, String path, V json) {
		return delegate.jsonStrappend(key, path, json);
	}

	@Override
	public RedisFuture<Long> jsonStrlen(K key, String path) {
		return delegate.jsonStrlen(key, path);
	}

	@Override
	public RedisFuture<Long> jsonArrappend(K key, String path, V... jsons) {
		return delegate.jsonArrappend(key, path, jsons);
	}

	@Override
	public RedisFuture<Long> jsonArrindex(K key, String path, V scalar) {
		return delegate.jsonArrindex(key, path, scalar);
	}

	@Override
	public RedisFuture<Long> jsonArrindex(K key, String path, V scalar, Slice slice) {
		return delegate.jsonArrindex(key, path, scalar, slice);
	}

	@Override
	public RedisFuture<Long> jsonArrinsert(K key, String path, long index, V... jsons) {
		return delegate.jsonArrinsert(key, path, index, jsons);
	}

	@Override
	public RedisFuture<Long> jsonArrlen(K key) {
		return delegate.jsonArrlen(key);
	}

	@Override
	public RedisFuture<Long> jsonArrlen(K key, String path) {
		return delegate.jsonArrlen(key, path);
	}

	@Override
	public RedisFuture<V> jsonArrpop(K key) {
		return delegate.jsonArrpop(key);
	}

	@Override
	public RedisFuture<V> jsonArrpop(K key, ArrpopOptions<K> options) {
		return delegate.jsonArrpop(key, options);
	}

	@Override
	public RedisFuture<Long> jsonArrtrim(K key, String path, long start, long stop) {
		return delegate.jsonArrtrim(key, path, start, stop);
	}

	@Override
	public RedisFuture<List<K>> jsonObjkeys(K key) {
		return delegate.jsonObjkeys(key);
	}

	@Override
	public RedisFuture<List<K>> jsonObjkeys(K key, String path) {
		return delegate.jsonObjkeys(key, path);
	}

	@Override
	public RedisFuture<Long> jsonObjlen(K key) {
		return delegate.jsonObjlen(key);
	}

	@Override
	public RedisFuture<Long> jsonObjlen(K key, String path) {
		return delegate.jsonObjlen(key, path);
	}

	@Override
	public RedisFuture<Boolean> bfAdd(K key, V item) { return delegate.bfAdd(key, item); }

	@Override
	public RedisFuture<Long> bfCard(K key) { return delegate.bfCard(key); }

	@Override
	public RedisFuture<Boolean> bfExists(K key, V item) { return delegate.bfExists(key, item); }

	@Override
	public RedisFuture<BfInfo> bfInfo(K key) { return delegate.bfInfo(key);	}

	@Override
	public RedisFuture<Long> bfInfo(K key, BfInfoType infoType) { return delegate.bfInfo(key, infoType); }

	@Override
	public RedisFuture<List<Boolean>> bfInsert(K key, V... items) { return delegate.bfInsert(key,items);	}

	@Override
	public RedisFuture<List<Boolean>> bfInsert(K key, BfInsertOptions options, V... items) { return delegate.bfInsert(key, options, items); }

	@Override
	public RedisFuture<List<Boolean>> bfMAdd(K key, V... items) { return delegate.bfMAdd(key, items); }

	@Override
	public RedisFuture<List<Boolean>> bfMExists(K key, V... items) { return delegate.bfMExists(key, items); }

	@Override
	public RedisFuture<String> bfReserve(K key, BfConfig config) { return delegate.bfReserve(key, config); }

	@Override
	public RedisFuture<Boolean> cfAdd(K key, V item) { return delegate.cfAdd(key,item);	}

	@Override
	public RedisFuture<Boolean> cfAddNx(K key, V item) { return delegate.cfAddNx(key,item);	}

	@Override
	public RedisFuture<Long> cfCount(K key, V item) { return delegate.cfCount(key,item); }

	@Override
	public RedisFuture<Boolean> cfDel(K key, V item) { return delegate.cfDel(key,item);	}

	@Override
	public RedisFuture<Boolean> cfExists(K key, V item) { return delegate.cfExists(key,item); }

	@Override
	public RedisFuture<CfInfo> cfInfo(K key) { return delegate.cfInfo(key);	}

	@Override
	public RedisFuture<List<Long>> cfInsert(K key, V[] items) { return delegate.cfInsert(key,items); }

	@Override
	public RedisFuture<List<Long>> cfInsert(K key, CfInsertOptions options, V... items) { return delegate.cfInsert(key,options, items); }

	@Override
	public RedisFuture<List<Long>> cfInsertNx(K key, V... items) { return delegate.cfInsertNx(key,items); }

	@Override
	public RedisFuture<List<Long>> cfInsertNx(K key, CfInsertOptions options, V... items) { return delegate.cfInsertNx(key,options,items); }

	@Override
	public RedisFuture<List<Boolean>> cfMExists(K key, V... items) { return delegate.cfMExists(key,items); }

	@Override
	public RedisFuture<String> cfReserve(K key, Long capacity) { return delegate.cfReserve(key,capacity); }

	@Override
	public RedisFuture<String> cfReserve(K key, CfReserveOptions options) { return delegate.cfReserve(key, options); }

	@Override
	public RedisFuture<Long> cmsIncrBy(K key, V item, long increment) { return delegate.cmsIncrBy(key,item,increment);	}

	@Override
	public RedisFuture<List<Long>> cmsIncrBy(K key, Map<V, Long> increments) { return delegate.cmsIncrBy(key,increments); }

	@Override
	public RedisFuture<String> cmsInitByProb(K key, double error, double probability) { return delegate.cmsInitByProb(key,error,probability); }

	@Override
	public RedisFuture<String> cmsInitByDim(K key, long width, long depth) { return delegate.cmsInitByDim(key, width, depth); }

	@Override
	public RedisFuture<List<Long>> cmsQuery(K key, V... items) { return delegate.cmsQuery(key,items); }

	@Override
	public RedisFuture<String> cmsMerge(K destKey, K... keys) { return delegate.cmsMerge(destKey,keys);	}

	@Override
	public RedisFuture<String> cmsMerge(K destKey, Map<K, Long> keyWeightMap) { return delegate.cmsMerge(destKey,keyWeightMap);	}

	@Override
	public RedisFuture<CmsInfo> cmsInfo(K key) { return delegate.cmsInfo(key); }

	@Override
	public RedisFuture<List<Optional<V>>> topKAdd(K key, V... items) { return delegate.topKAdd(key, items); }

	@Override
	public RedisFuture<List<Optional<V>>> topKIncrBy(K key, Map<V, Long> increments) { return delegate.topKIncrBy(key, increments);	}

	@Override
	public RedisFuture<TopKInfo> topKInfo(K key) { return delegate.topKInfo(key); }

	@Override
	public RedisFuture<List<String>> topKList(K key) { return delegate.topKList(key); }

	@Override
	public RedisFuture<Map<String, Long>> topKListWithScores(K key) { return delegate.topKListWithScores(key); }

	@Override
	public RedisFuture<List<Boolean>> topKQuery(K key, V... items) { return delegate.topKQuery(key, items);	}

	@Override
	public RedisFuture<String> topKReserve(K key, long k) { return delegate.topKReserve(key, k); }

	@Override
	public RedisFuture<String> topKReserve(K key, long k, long width, long depth, double decay) { return delegate.topKReserve(key,k,width,depth,decay);	}

	@Override
	public RedisFuture<String> tDigestAdd(K key, double... value) { return delegate.tDigestAdd(key,value);	}

	@Override
	public RedisFuture<List<Double>> tDigestByRank(K key, long... ranks) { return delegate.tDigestByRank(key, ranks); }

	@Override
	public RedisFuture<List<Double>> tDigestByRevRank(K key, long... revRanks) { return delegate.tDigestByRevRank(key, revRanks); }

	@Override
	public RedisFuture<List<Double>> tDigestCdf(K key, double... values) { return delegate.tDigestCdf(key, values);	}

	@Override
	public RedisFuture<String> tDigestCreate(K key) { return delegate.tDigestCreate(key); }

	@Override
	public RedisFuture<String> tDigestCreate(K key, long compression) { return delegate.tDigestCreate(key, compression); }

	@Override
	public RedisFuture<TDigestInfo> tDigestInfo(K key) { return delegate.tDigestInfo(key);	}

	@Override
	public RedisFuture<Double> tDigestMax(K key) { return delegate.tDigestMax(key);	}

	@Override
	public RedisFuture<String> tDigestMerge(K destinationKey, K... sourceKeys) { return delegate.tDigestMerge(destinationKey, sourceKeys);	}

	@Override
	public RedisFuture<String> tDigestMerge(K destinationKey, TDigestMergeOptions options, K... sourceKeys) { return delegate.tDigestMerge(destinationKey, options, sourceKeys); }

	@Override
	public RedisFuture<Double> tDigestMin(K key) { return delegate.tDigestMin(key);	}

	@Override
	public RedisFuture<List<Double>> tDigestQuantile(K key, double... quantiles) { return delegate.tDigestQuantile(key, quantiles);	}

	@Override
	public RedisFuture<List<Long>> tDigestRank(K key, double... values) { return delegate.tDigestRank(key, values);	}

	@Override
	public RedisFuture<String> tDigestReset(K key) { return delegate.tDigestReset(key);	}

	@Override
	public RedisFuture<List<Long>> tDigestRevRank(K key, double... values) { return delegate.tDigestRevRank(key, values);	}

	@Override
	public RedisFuture<Double> tDigestTrimmedMean(K key, double lowCutQuantile, double highCutQuantile) { return delegate.tDigestTrimmedMean(key,lowCutQuantile,highCutQuantile); }
}
