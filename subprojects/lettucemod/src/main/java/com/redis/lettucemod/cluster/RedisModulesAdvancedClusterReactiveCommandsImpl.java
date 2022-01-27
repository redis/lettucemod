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
import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.SetMode;
import com.redis.lettucemod.output.ExecutionResults;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateResults;
import com.redis.lettucemod.search.AggregateWithCursorResults;
import com.redis.lettucemod.search.Cursor;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.KeySample;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.KeyValue;
import io.lettuce.core.cluster.RedisAdvancedClusterReactiveCommandsImpl;
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
	public Mono<String> abortexecution(String id) {
		return delegate.abortexecution(id);
	}

	@Override
	public Flux<V> configget(K... keys) {
		return delegate.configget(keys);
	}

	@Override
	public Flux<V> configset(Map<K, V> map) {
		return delegate.configset(map);
	}

	@Override
	public Mono<String> dropexecution(String id) {
		return delegate.dropexecution(id);
	}

	@Override
	public Flux<Execution> dumpexecutions() {
		return delegate.dumpexecutions();
	}

	@Override
	public Flux<Registration> dumpregistrations() {
		return delegate.dumpregistrations();
	}

	@Override
	public Mono<ExecutionDetails> getexecution(String id) {
		return delegate.getexecution(id);
	}

	@Override
	public Mono<ExecutionDetails> getexecution(String id, ExecutionMode mode) {
		return delegate.getexecution(id, mode);
	}

	@Override
	public Mono<ExecutionResults> getresults(String id) {
		return delegate.getresults(id);
	}

	@Override
	public Mono<ExecutionResults> getresultsBlocking(String id) {
		return delegate.getresultsBlocking(id);
	}

	@Override
	public Mono<ExecutionResults> pyexecute(String function, V... requirements) {
		return delegate.pyexecute(function, requirements);
	}

	@Override
	public Mono<String> pyexecuteUnblocking(String function, V... requirements) {
		return delegate.pyexecuteUnblocking(function, requirements);
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
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).create(index, options, fields));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Mono<String> dropindex(K index) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).dropindex(index));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Mono<String> dropindexDeleteDocs(K index) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).dropindexDeleteDocs(index));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Mono<String> alter(K index, Field field) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).alter(index, field));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Flux<Object> indexInfo(K index) {
		return delegate.indexInfo(index);
	}

	@Override
	public Mono<String> aliasadd(K name, K index) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).aliasadd(name, index));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Mono<String> aliasupdate(K name, K index) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).aliasupdate(name, index));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Mono<String> aliasdel(K name) {
		Map<String, Publisher<String>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).aliasdel(name));
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
	public Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, Cursor cursor,
			AggregateOptions<K, V> options) {
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
	public Flux<V> tagvals(K index, K field) {
		return delegate.tagvals(index, field);
	}

	@Override
	public Mono<Long> sugadd(K key, V string, double score) {
		return delegate.sugadd(key, string, score);
	}

	@Override
	public Mono<Long> sugaddIncr(K key, V string, double score) {
		return delegate.sugaddIncr(key, string, score);
	}

	@Override
	public Mono<Long> sugadd(K key, V string, double score, V payload) {
		return delegate.sugadd(key, string, score, payload);
	}

	@Override
	public Mono<Long> sugaddIncr(K key, V string, double score, V payload) {
		return delegate.sugaddIncr(key, string, score, payload);
	}

	@Override
	public Mono<Long> sugadd(K key, Suggestion<V> suggestion) {
		return delegate.sugadd(key, suggestion);
	}

	@Override
	public Mono<Long> sugaddIncr(K key, Suggestion<V> suggestion) {
		return delegate.sugaddIncr(key, suggestion);
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
		Map<String, Publisher<Long>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).dictadd(dict, terms));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Mono<Long> dictdel(K dict, V... terms) {
		Map<String, Publisher<Long>> publishers = executeOnUpstream(
				commands -> ((RedisModulesReactiveCommands<K, V>) commands).dictdel(dict, terms));
		return Flux.merge(publishers.values()).last();
	}

	@Override
	public Flux<V> dictdump(K dict) {
		return delegate.dictdump(dict);
	}

	@Override
	public Mono<String> create(K key, CreateOptions<K, V> options) {
		return delegate.create(key, options);
	}

	@Override
	public Mono<String> alter(K key, CreateOptions<K, V> options) {
		return delegate.alter(key, options);
	}

	@Override
	public Mono<Long> add(K key, long timestamp, double value) {
		return delegate.add(key, timestamp, value);
	}

	@Override
	public Mono<Long> add(K key, long timestamp, double value, CreateOptions<K, V> options) {
		return delegate.add(key, timestamp, value, options);
	}

	@Override
	public Mono<Long> addAutoTimestamp(K key, double value) {
		return delegate.addAutoTimestamp(key, value);
	}

	@Override
	public Mono<Long> addAutoTimestamp(K key, double value, CreateOptions<K, V> options) {
		return delegate.addAutoTimestamp(key, value, options);
	}

	@Override
	public Mono<Long> add(K key, Sample sample) {
		return delegate.add(key, sample);
	}

	@Override
	public Mono<Long> add(K key, Sample sample, CreateOptions<K, V> options) {
		return delegate.add(key, sample, options);
	}

	@Override
	public Flux<Long> madd(KeySample<K>... samples) {
		return delegate.madd(samples);
	}

	@Override
	public Mono<Long> incrby(K key, double value, Long timestamp, CreateOptions<K, V> options) {
		return delegate.incrby(key, value, timestamp, options);
	}

	@Override
	public Mono<Long> decrby(K key, double value, Long timestamp, CreateOptions<K, V> options) {
		return delegate.decrby(key, value, timestamp, options);
	}

	@Override
	public Mono<Long> incrbyAutoTimestamp(K key, double value, CreateOptions<K, V> options) {
		return delegate.incrbyAutoTimestamp(key, value, options);
	}

	@Override
	public Mono<Long> decrbyAutoTimestamp(K key, double value, CreateOptions<K, V> options) {
		return delegate.decrbyAutoTimestamp(key, value, options);
	}

	@Override
	public Mono<String> createrule(K sourceKey, K destKey, Aggregation aggregation) {
		return delegate.createrule(sourceKey, destKey, aggregation);
	}

	@Override
	public Mono<String> deleterule(K sourceKey, K destKey) {
		return delegate.deleterule(sourceKey, destKey);
	}

	@Override
	public Flux<Sample> range(K key, RangeOptions options) {
		return delegate.range(key, options);
	}

	@Override
	public Flux<Sample> revrange(K key, RangeOptions range) {
		return delegate.revrange(key, range);
	}

	@Override
	public Flux<RangeResult<K, V>> mrange(RangeOptions options, V... filters) {
		return delegate.mrange(options, filters);
	}

	@Override
	public Flux<RangeResult<K, V>> mrangeWithLabels(RangeOptions options, V... filters) {
		return delegate.mrangeWithLabels(options, filters);
	}

	@Override
	public Flux<RangeResult<K, V>> mrevrange(RangeOptions options, V... filters) {
		return delegate.mrevrange(options, filters);
	}

	@Override
	public Flux<RangeResult<K, V>> mrevrangeWithLabels(RangeOptions options, V... filters) {
		return delegate.mrevrangeWithLabels(options, filters);
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
	public Mono<Long> jsonDel(K key, K path) {
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
	public Flux<KeyValue<K, V>> jsonMget(K path, K... keys) {
		return mget(path, Arrays.asList(keys));
	}

	public Flux<KeyValue<K, V>> mget(K path, Iterable<K> keys) {

		List<K> keyList = LettuceLists.newList(keys);
		Map<Integer, List<K>> partitioned = ModulesSlotHash.partition(codec, keyList);

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
	public Mono<String> jsonSet(K key, K path, V json) {
		return delegate.jsonSet(key, path, json);
	}

	@Override
	public Mono<String> jsonSet(K key, K path, V json, SetMode mode) {
		return delegate.jsonSet(key, path, json, mode);
	}

	@Override
	public Mono<String> jsonType(K key) {
		return delegate.jsonType(key);
	}

	@Override
	public Mono<String> jsonType(K key, K path) {
		return delegate.jsonType(key, path);
	}

	@Override
	public Mono<V> numincrby(K key, K path, double number) {
		return delegate.numincrby(key, path, number);
	}

	@Override
	public Mono<V> nummultby(K key, K path, double number) {
		return delegate.nummultby(key, path, number);
	}

	@Override
	public Mono<Long> strappend(K key, V json) {
		return delegate.strappend(key, json);
	}

	@Override
	public Mono<Long> strappend(K key, K path, V json) {
		return delegate.strappend(key, path, json);
	}

	@Override
	public Mono<Long> strlen(K key, K path) {
		return delegate.strlen(key, path);
	}

	@Override
	public Mono<Long> arrappend(K key, K path, V... jsons) {
		return delegate.arrappend(key, path, jsons);
	}

	@Override
	public Mono<Long> arrindex(K key, K path, V scalar) {
		return delegate.arrindex(key, path, scalar);
	}

	@Override
	public Mono<Long> arrindex(K key, K path, V scalar, long start) {
		return delegate.arrindex(key, path, scalar, start);
	}

	@Override
	public Mono<Long> arrindex(K key, K path, V scalar, long start, long stop) {
		return delegate.arrindex(key, path, scalar, start, stop);
	}

	@Override
	public Mono<Long> arrinsert(K key, K path, long index, V... jsons) {
		return delegate.arrinsert(key, path, index, jsons);
	}

	@Override
	public Mono<Long> arrlen(K key) {
		return delegate.arrlen(key);
	}

	@Override
	public Mono<Long> arrlen(K key, K path) {
		return delegate.arrlen(key, path);
	}

	@Override
	public Mono<V> arrpop(K key) {
		return delegate.arrpop(key);
	}

	@Override
	public Mono<V> arrpop(K key, K path) {
		return delegate.arrpop(key, path);
	}

	@Override
	public Mono<V> arrpop(K key, K path, long index) {
		return delegate.arrpop(key, path, index);
	}

	@Override
	public Mono<Long> arrtrim(K key, K path, long start, long stop) {
		return delegate.arrtrim(key, path, start, stop);
	}

	@Override
	public Flux<K> objkeys(K key) {
		return delegate.objkeys(key);
	}

	@Override
	public Flux<K> objkeys(K key, K path) {
		return delegate.objkeys(key, path);
	}

	@Override
	public Mono<Long> objlen(K key) {
		return delegate.objlen(key);
	}

	@Override
	public Mono<Long> objlen(K key, K path) {
		return delegate.objlen(key, path);
	}

	@Override
	public Mono<Long> pfaddNoValue(K key) {
		return delegate.pfaddNoValue(key);
	}

}
