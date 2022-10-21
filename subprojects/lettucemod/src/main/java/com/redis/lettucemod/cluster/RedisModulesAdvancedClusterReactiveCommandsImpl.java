package com.redis.lettucemod.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.reactivestreams.Publisher;

import com.redis.lettucemod.RedisModulesReactiveCommandsImpl;
import com.redis.lettucemod.api.reactive.RedisModulesReactiveCommands;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.lettucemod.cluster.api.reactive.RedisModulesAdvancedClusterReactiveCommands;
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
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;

import io.lettuce.core.KeyValue;
import io.lettuce.core.cluster.RedisAdvancedClusterReactiveCommandsImpl;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceLists;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
public class RedisModulesAdvancedClusterReactiveCommandsImpl<K, V> extends
		RedisAdvancedClusterReactiveCommandsImpl<K, V> implements RedisModulesAdvancedClusterReactiveCommands<K, V> {

	private final RedisModulesReactiveCommandsImpl<K, V> delegate;
	private final RedisCodec<K, V> codec;

	public RedisModulesAdvancedClusterReactiveCommandsImpl(StatefulRedisModulesClusterConnection<K, V> connection,
			RedisCodec<K, V> codec) {
		super(connection, codec);
		this.codec = codec;
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
	public Mono<String> rgAbortexecution(String id) {
		return delegate.rgAbortexecution(id);
	}

	@Override
	public Flux<V> rgConfigget(K... keys) {
		return delegate.rgConfigget(keys);
	}

	@Override
	public Flux<V> rgConfigset(Map<K, V> map) {
		return delegate.rgConfigset(map);
	}

	@Override
	public Mono<String> rgDropexecution(String id) {
		return delegate.rgDropexecution(id);
	}

	@Override
	public Flux<Execution> rgDumpexecutions() {
		return delegate.rgDumpexecutions();
	}

	@Override
	public Flux<Registration> rgDumpregistrations() {
		return delegate.rgDumpregistrations();
	}

	@Override
	public Mono<ExecutionDetails> rgGetexecution(String id) {
		return delegate.rgGetexecution(id);
	}

	@Override
	public Mono<ExecutionDetails> rgGetexecution(String id, ExecutionMode mode) {
		return delegate.rgGetexecution(id, mode);
	}

	@Override
	public Mono<ExecutionResults> rgGetresults(String id) {
		return delegate.rgGetresults(id);
	}

	@Override
	public Mono<ExecutionResults> rgGetresultsblocking(String id) {
		return delegate.rgGetresultsblocking(id);
	}

	@Override
	public Mono<ExecutionResults> rgPyexecute(String function, V... requirements) {
		return delegate.rgPyexecute(function, requirements);
	}

	@Override
	public Mono<String> rgPyexecuteUnblocking(String function, V... requirements) {
		return delegate.rgPyexecuteUnblocking(function, requirements);
	}

	@Override
	public Flux<Object> rgTrigger(String trigger, V... args) {
		return delegate.rgTrigger(trigger, args);
	}

	@Override
	public Mono<String> rgUnregister(String id) {
		return delegate.rgUnregister(id);
	}

	@Override
	public Mono<String> ftCreate(K index, Field<K>... fields) {
		return ftCreate(index, null, fields);
	}

	@Override
	public Mono<String> ftCreate(K index, com.redis.lettucemod.search.CreateOptions<K, V> options, Field<K>... fields) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).ftCreate(index, options, fields));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Mono<String> ftDropindex(K index) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).ftDropindex(index));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Mono<String> ftDropindexDeleteDocs(K index) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).ftDropindexDeleteDocs(index));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Mono<String> ftAlter(K index, Field<K> field) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).ftAlter(index, field));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Flux<Object> ftInfo(K index) {
		return delegate.ftInfo(index);
	}

	@Override
	public Mono<String> ftAliasadd(K name, K index) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).ftAliasadd(name, index));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Mono<String> ftAliasupdate(K name, K index) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).ftAliasupdate(name, index));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Mono<String> ftAliasdel(K name) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).ftAliasdel(name));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Flux<K> ftList() {
		return delegate.ftList();
	}

	@Override
	public Mono<SearchResults<K, V>> ftSearch(K index, V query) {
		return delegate.ftSearch(index, query);
	}

	@Override
	public Mono<SearchResults<K, V>> ftSearch(K index, V query, SearchOptions<K, V> options) {
		return delegate.ftSearch(index, query, options);
	}

	@Override
	public Mono<AggregateResults<K>> ftAggregate(K index, V query) {
		return delegate.ftAggregate(index, query);
	}

	@Override
	public Mono<AggregateResults<K>> ftAggregate(K index, V query, AggregateOptions<K, V> options) {
		return delegate.ftAggregate(index, query, options);
	}

	@Override
	public Mono<AggregateWithCursorResults<K>> ftAggregate(K index, V query, CursorOptions cursor) {
		return delegate.ftAggregate(index, query, cursor);
	}

	@Override
	public Mono<AggregateWithCursorResults<K>> ftAggregate(K index, V query, CursorOptions cursor,
			AggregateOptions<K, V> options) {
		return delegate.ftAggregate(index, query, cursor, options);
	}

	@Override
	public Mono<AggregateWithCursorResults<K>> ftCursorRead(K index, long cursor) {
		return delegate.ftCursorRead(index, cursor);
	}

	@Override
	public Mono<AggregateWithCursorResults<K>> ftCursorRead(K index, long cursor, long count) {
		return delegate.ftCursorRead(index, cursor, count);
	}

	@Override
	public Mono<String> ftCursorDelete(K index, long cursor) {
		return delegate.ftCursorDelete(index, cursor);
	}

	@Override
	public Flux<V> ftTagvals(K index, K field) {
		return delegate.ftTagvals(index, field);
	}

	@Override
	public Mono<Long> ftSugadd(K key, Suggestion<V> suggestion) {
		return delegate.ftSugadd(key, suggestion);
	}

	@Override
	public Mono<Long> ftSugaddIncr(K key, Suggestion<V> suggestion) {
		return delegate.ftSugaddIncr(key, suggestion);
	}

	@Override
	public Flux<Suggestion<V>> ftSugget(K key, V prefix) {
		return delegate.ftSugget(key, prefix);
	}

	@Override
	public Flux<Suggestion<V>> ftSugget(K key, V prefix, SuggetOptions options) {
		return delegate.ftSugget(key, prefix, options);
	}

	@Override
	public Mono<Boolean> ftSugdel(K key, V string) {
		return delegate.ftSugdel(key, string);
	}

	@Override
	public Mono<Long> ftSuglen(K key) {
		return delegate.ftSuglen(key);
	}

	@Override
	public Mono<Long> ftDictadd(K dict, V... terms) {
		Map<String, Publisher<Long>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).ftDictadd(dict, terms));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Mono<Long> ftDictdel(K dict, V... terms) {
		Map<String, Publisher<Long>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).ftDictdel(dict, terms));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Flux<V> ftDictdump(K dict) {
		return delegate.ftDictdump(dict);
	}

	@Override
	public Mono<String> tsCreate(K key, CreateOptions<K, V> options) {
		return delegate.tsCreate(key, options);
	}

	@Override
	public Mono<String> tsAlter(K key, AlterOptions<K, V> options) {
		return delegate.tsAlter(key, options);
	}

	@Override
	public Mono<Long> tsAdd(K key, Sample sample) {
		return delegate.tsAdd(key, sample);
	}

	@Override
	public Mono<Long> tsAdd(K key, Sample sample, AddOptions<K, V> options) {
		return delegate.tsAdd(key, sample, options);
	}

	@Override
	public Flux<Long> tsMadd(KeySample<K>... samples) {
		return delegate.tsMadd(samples);
	}

	@Override
	public Mono<Long> tsDecrby(K key, double value) {
		return delegate.tsDecrby(key, value);
	}

	@Override
	public Mono<Long> tsDecrby(K key, double value, IncrbyOptions<K, V> options) {
		return delegate.tsDecrby(key, value, options);
	}

	@Override
	public Mono<Long> tsIncrby(K key, double value) {
		return delegate.tsIncrby(key, value);
	}

	@Override
	public Mono<Long> tsIncrby(K key, double value, IncrbyOptions<K, V> options) {
		return delegate.tsIncrby(key, value, options);
	}

	@Override
	public Mono<String> tsCreaterule(K sourceKey, K destKey, CreateRuleOptions options) {
		return delegate.tsCreaterule(sourceKey, destKey, options);
	}

	@Override
	public Mono<String> tsDeleterule(K sourceKey, K destKey) {
		return delegate.tsDeleterule(sourceKey, destKey);
	}

	@Override
	public Flux<Sample> tsRange(K key, TimeRange range) {
		return delegate.tsRange(key, range);
	}

	@Override
	public Flux<Sample> tsRange(K key, TimeRange range, RangeOptions options) {
		return delegate.tsRange(key, range, options);
	}

	@Override
	public Flux<Sample> tsRevrange(K key, TimeRange range) {
		return delegate.tsRevrange(key, range);
	}

	@Override
	public Flux<Sample> tsRevrange(K key, TimeRange range, RangeOptions options) {
		return delegate.tsRevrange(key, range, options);
	}

	@Override
	public Flux<RangeResult<K, V>> tsMrange(TimeRange range) {
		return delegate.tsMrange(range);
	}

	@Override
	public Flux<RangeResult<K, V>> tsMrange(TimeRange range, MRangeOptions<K, V> options) {
		return delegate.tsMrange(range, options);
	}

	@Override
	public Flux<RangeResult<K, V>> tsMrevrange(TimeRange range) {
		return delegate.tsMrevrange(range);
	}

	@Override
	public Flux<RangeResult<K, V>> tsMrevrange(TimeRange range, MRangeOptions<K, V> options) {
		return delegate.tsMrevrange(range, options);
	}

	@Override
	public Mono<Sample> tsGet(K key) {
		return delegate.tsGet(key);
	}

	@Override
	public Flux<GetResult<K, V>> tsMget(V... filters) {
		return delegate.tsMget(filters);
	}

	@Override
	public Flux<GetResult<K, V>> tsMgetWithLabels(V... filters) {
		return delegate.tsMgetWithLabels(filters);
	}

	@Override
	public Flux<Object> tsInfo(K key) {
		return delegate.tsInfo(key);
	}

	@Override
	public Flux<Object> tsInfoDebug(K key) {
		return delegate.tsInfoDebug(key);
	}

	@Override
	public Mono<Long> jsonDel(K key) {
		return delegate.jsonDel(key);
	}

	@Override
	public Mono<Long> jsonDel(K key, String path) {
		return delegate.jsonDel(key, path);
	}

	@Override
	public Mono<V> jsonGet(K key, K... paths) {
		return delegate.jsonGet(key, paths);
	}

	@Override
	public Mono<V> jsonGet(K key, GetOptions options, K... paths) {
		return delegate.jsonGet(key, options, paths);
	}

	@Override
	public Flux<KeyValue<K, V>> jsonMget(String path, K... keys) {
		return mget(path, Arrays.asList(keys));
	}

	public Flux<KeyValue<K, V>> mget(String path, Iterable<K> keys) {

		List<K> keyList = LettuceLists.newList(keys);
		Map<Integer, List<K>> partitioned = SlotHash.partition(codec, keyList);

		if (partitioned.size() < 2) {
			return delegate.mget(path, keyList);
		}

		List<Publisher<KeyValue<K, V>>> publishers = new ArrayList<>();

		for (Map.Entry<Integer, List<K>> entry : partitioned.entrySet()) {
			publishers.add(delegate.mget(path, entry.getValue()));
		}

		Flux<KeyValue<K, V>> fluxes = Flux.concat(publishers);

		Mono<List<KeyValue<K, V>>> map = fluxes.collectList().map(vs -> {

			KeyValue<K, V>[] values = new KeyValue[vs.size()];
			int offset = 0;
			for (Map.Entry<Integer, List<K>> entry : partitioned.entrySet()) {

				for (int i = 0; i < keyList.size(); i++) {

					int index = entry.getValue().indexOf(keyList.get(i));
					if (index == -1) {
						continue;
					}

					values[i] = vs.get(offset + index);
				}

				offset += entry.getValue().size();
			}

			return Arrays.asList(values);
		});

		return map.flatMapIterable(keyValues -> keyValues);
	}

	@Override
	public Mono<String> jsonSet(K key, String path, V json) {
		return delegate.jsonSet(key, path, json);
	}

	@Override
	public Mono<String> jsonSet(K key, String path, V json, SetMode mode) {
		return delegate.jsonSet(key, path, json, mode);
	}

	@Override
	public Mono<String> jsonType(K key) {
		return delegate.jsonType(key);
	}

	@Override
	public Mono<String> jsonType(K key, String path) {
		return delegate.jsonType(key, path);
	}

	@Override
	public Mono<V> jsonNumincrby(K key, String path, double number) {
		return delegate.jsonNumincrby(key, path, number);
	}

	@Override
	public Mono<V> jsonNummultby(K key, String path, double number) {
		return delegate.jsonNummultby(key, path, number);
	}

	@Override
	public Mono<Long> jsonStrappend(K key, V json) {
		return delegate.jsonStrappend(key, json);
	}

	@Override
	public Mono<Long> jsonStrappend(K key, String path, V json) {
		return delegate.jsonStrappend(key, path, json);
	}

	@Override
	public Mono<Long> jsonStrlen(K key, String path) {
		return delegate.jsonStrlen(key, path);
	}

	@Override
	public Mono<Long> jsonArrappend(K key, String path, V... jsons) {
		return delegate.jsonArrappend(key, path, jsons);
	}

	@Override
	public Mono<Long> jsonArrindex(K key, String path, V scalar) {
		return delegate.jsonArrindex(key, path, scalar);
	}

	@Override
	public Mono<Long> jsonArrindex(K key, String path, V scalar, Slice slice) {
		return delegate.jsonArrindex(key, path, scalar, slice);
	}

	@Override
	public Mono<Long> jsonArrinsert(K key, String path, long index, V... jsons) {
		return delegate.jsonArrinsert(key, path, index, jsons);
	}

	@Override
	public Mono<Long> jsonArrlen(K key) {
		return delegate.jsonArrlen(key);
	}

	@Override
	public Mono<Long> jsonArrlen(K key, String path) {
		return delegate.jsonArrlen(key, path);
	}

	@Override
	public Mono<V> jsonArrpop(K key) {
		return delegate.jsonArrpop(key);
	}

	@Override
	public Mono<V> jsonArrpop(K key, ArrpopOptions<K> options) {
		return delegate.jsonArrpop(key, options);
	}

	@Override
	public Mono<Long> jsonArrtrim(K key, String path, long start, long stop) {
		return delegate.jsonArrtrim(key, path, start, stop);
	}

	@Override
	public Flux<K> jsonObjkeys(K key) {
		return delegate.jsonObjkeys(key);
	}

	@Override
	public Flux<K> jsonObjkeys(K key, String path) {
		return delegate.jsonObjkeys(key, path);
	}

	@Override
	public Mono<Long> jsonObjlen(K key) {
		return delegate.jsonObjlen(key);
	}

	@Override
	public Mono<Long> jsonObjlen(K key, String path) {
		return delegate.jsonObjlen(key, path);
	}

}
