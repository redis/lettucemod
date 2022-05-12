package com.redis.lettucemod;

import java.util.Map;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.reactive.RedisModulesReactiveCommands;
import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.ExecutionMode;
import com.redis.lettucemod.gears.RedisGearsCommandBuilder;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.RedisJSONCommandBuilder;
import com.redis.lettucemod.json.SetMode;
import com.redis.lettucemod.output.ExecutionResults;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateResults;
import com.redis.lettucemod.search.AggregateWithCursorResults;
import com.redis.lettucemod.search.CursorOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.RediSearchCommandBuilder;
import com.redis.lettucemod.search.SearchOptions;
import com.redis.lettucemod.search.SearchResults;
import com.redis.lettucemod.search.Suggestion;
import com.redis.lettucemod.search.SuggetOptions;
import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.KeySample;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.RedisTimeSeriesCommandBuilder;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisReactiveCommandsImpl;
import io.lettuce.core.codec.RedisCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
public class RedisModulesReactiveCommandsImpl<K, V> extends RedisReactiveCommandsImpl<K, V>
		implements RedisModulesReactiveCommands<K, V> {

	private final StatefulRedisModulesConnection<K, V> connection;
	private final RedisTimeSeriesCommandBuilder<K, V> timeSeriesCommandBuilder;
	private final RedisGearsCommandBuilder<K, V> gearsCommandBuilder;
	private final RediSearchCommandBuilder<K, V> searchCommandBuilder;
	private final RedisJSONCommandBuilder<K, V> jsonCommandBuilder;

	public RedisModulesReactiveCommandsImpl(StatefulRedisModulesConnection<K, V> connection, RedisCodec<K, V> codec) {
		super(connection, codec);
		this.connection = connection;
		this.gearsCommandBuilder = new RedisGearsCommandBuilder<>(codec);
		this.timeSeriesCommandBuilder = new RedisTimeSeriesCommandBuilder<>(codec);
		this.searchCommandBuilder = new RediSearchCommandBuilder<>(codec);
		this.jsonCommandBuilder = new RedisJSONCommandBuilder<>(codec);
	}

	@Override
	public StatefulRedisModulesConnection<K, V> getStatefulConnection() {
		return connection;
	}

	@Override
	public Mono<ExecutionResults> pyexecute(String function, V... requirements) {
		return createMono(() -> gearsCommandBuilder.pyExecute(function, requirements));
	}

	@Override
	public Mono<String> pyexecuteUnblocking(String function, V... requirements) {
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
	public Mono<String> abortexecution(String id) {
		return createMono(() -> gearsCommandBuilder.abortExecution(id));
	}

	@Override
	public Flux<V> configget(K... keys) {
		return createDissolvingFlux(() -> gearsCommandBuilder.configGet(keys));
	}

	@Override
	public Flux<V> configset(Map<K, V> map) {
		return createDissolvingFlux(() -> gearsCommandBuilder.configSet(map));
	}

	@Override
	public Mono<String> dropexecution(String id) {
		return createMono(() -> gearsCommandBuilder.dropExecution(id));
	}

	@Override
	public Flux<Execution> dumpexecutions() {
		return createDissolvingFlux(gearsCommandBuilder::dumpExecutions);
	}

	@Override
	public Flux<Registration> dumpregistrations() {
		return createDissolvingFlux(gearsCommandBuilder::dumpRegistrations);
	}

	@Override
	public Mono<ExecutionDetails> getexecution(String id) {
		return createMono(() -> gearsCommandBuilder.getExecution(id));
	}

	@Override
	public Mono<ExecutionDetails> getexecution(String id, ExecutionMode mode) {
		return createMono(() -> gearsCommandBuilder.getExecution(id, mode));
	}

	@Override
	public Mono<ExecutionResults> getresults(String id) {
		return createMono(() -> gearsCommandBuilder.getResults(id));
	}

	@Override
	public Mono<ExecutionResults> getresultsBlocking(String id) {
		return createMono(() -> gearsCommandBuilder.getResultsBlocking(id));
	}

	@Override
	public Mono<String> create(K key, CreateOptions options, Label<K, V>... labels) {
		return createMono(() -> timeSeriesCommandBuilder.create(key, options, labels));
	}

	@Override
	public Mono<String> alter(K key, CreateOptions options, Label<K,V>... labels) {
		return createMono(() -> timeSeriesCommandBuilder.alter(key, options, labels));
	}

	@Override
	public Mono<Long> add(K key, long timestamp, double value) {
		return createMono(() -> timeSeriesCommandBuilder.add(key, timestamp, value));
	}

	@Override
	public Mono<Long> add(K key, long timestamp, double value, CreateOptions options, Label<K,V>... labels) {
		return createMono(() -> timeSeriesCommandBuilder.add(key, timestamp, value, options, labels));
	}

	@Override
	public Mono<Long> addAutoTimestamp(K key, double value) {
		return createMono(() -> timeSeriesCommandBuilder.addAutoTimestamp(key, value));
	}

	@Override
	public Mono<Long> addAutoTimestamp(K key, double value, CreateOptions options, Label<K,V>... labels) {
		return createMono(() -> timeSeriesCommandBuilder.addAutoTimestamp(key, value, options, labels));
	}

	@Override
	public Mono<Long> add(K key, Sample sample) {
		return createMono(() -> timeSeriesCommandBuilder.add(key, sample));
	}

	@Override
	public Mono<Long> add(K key, Sample sample, CreateOptions options, Label<K,V>... labels) {
		return createMono(() -> timeSeriesCommandBuilder.add(key, sample, options, labels));
	}

	@Override
	public Mono<Long> incrby(K key, double value, Long timestamp, CreateOptions options, Label<K,V>... labels) {
		return createMono(() -> timeSeriesCommandBuilder.incrby(key, value, timestamp, options, labels));
	}

	@Override
	public Mono<Long> decrby(K key, double value, Long timestamp, CreateOptions options, Label<K,V>... labels) {
		return createMono(() -> timeSeriesCommandBuilder.decrby(key, value, timestamp, options, labels));
	}

	@Override
	public Mono<Long> incrbyAutoTimestamp(K key, double value, CreateOptions options, Label<K,V>... labels) {
		return createMono(() -> timeSeriesCommandBuilder.incrbyAutoTimestamp(key, value, options, labels));
	}

	@Override
	public Mono<Long> decrbyAutoTimestamp(K key, double value, CreateOptions options, Label<K,V>... labels) {
		return createMono(() -> timeSeriesCommandBuilder.decrbyAutoTimestamp(key, value, options, labels));
	}

	@Override
	public Flux<Long> madd(KeySample<K>... samples) {
		return createDissolvingFlux(() -> timeSeriesCommandBuilder.madd(samples));
	}

	@Override
	public Mono<String> createrule(K sourceKey, K destKey, Aggregation aggregation) {
		return createMono(() -> timeSeriesCommandBuilder.createRule(sourceKey, destKey, aggregation));
	}

	@Override
	public Mono<String> deleterule(K sourceKey, K destKey) {
		return createMono(() -> timeSeriesCommandBuilder.deleteRule(sourceKey, destKey));
	}

	@Override
	public Flux<Sample> range(K key, RangeOptions options) {
		return createDissolvingFlux(() -> timeSeriesCommandBuilder.range(key, options));
	}

	@Override
	public Flux<Sample> revrange(K key, RangeOptions options) {
		return createDissolvingFlux(() -> timeSeriesCommandBuilder.revrange(key, options));
	}

	@Override
	public Flux<RangeResult<K, V>> mrange(RangeOptions options, V... filters) {
		return createDissolvingFlux(() -> timeSeriesCommandBuilder.mrange(options, filters));
	}

	@Override
	public Flux<RangeResult<K, V>> mrevrange(RangeOptions options, V... filters) {
		return createDissolvingFlux(() -> timeSeriesCommandBuilder.mrevrange(options, filters));
	}

	@Override
	public Flux<RangeResult<K, V>> mrangeWithLabels(RangeOptions options, V... filters) {
		return createDissolvingFlux(() -> timeSeriesCommandBuilder.mrangeWithLabels(options, filters));
	}

	@Override
	public Flux<RangeResult<K, V>> mrevrangeWithLabels(RangeOptions options, V... filters) {
		return createDissolvingFlux(() -> timeSeriesCommandBuilder.mrevrangeWithLabels(options, filters));
	}

	@Override
	public Mono<Sample> tsGet(K key) {
		return createMono(() -> timeSeriesCommandBuilder.get(key));
	}

	@Override
	public Flux<GetResult<K, V>> tsMget(V... filters) {
		return createDissolvingFlux(() -> timeSeriesCommandBuilder.mget(false, filters));
	}

	@Override
	public Flux<GetResult<K, V>> tsMgetWithLabels(V... filters) {
		return createDissolvingFlux(() -> timeSeriesCommandBuilder.mget(true, filters));
	}

	@Override
	public Flux<Object> tsInfo(K key) {
		return createDissolvingFlux(() -> timeSeriesCommandBuilder.info(key, false));
	}

	@Override
	public Flux<Object> tsInfoDebug(K key) {
		return createDissolvingFlux(() -> timeSeriesCommandBuilder.info(key, true));
	}

	@Override
	public Mono<String> create(K index, Field... fields) {
		return create(index, null, fields);
	}

	@Override
	public Mono<String> create(K index, com.redis.lettucemod.search.CreateOptions<K,V> options, Field... fields) {
		return createMono(() -> searchCommandBuilder.create(index, options, fields));
	}

	@Override
	public Mono<String> dropindex(K index) {
		return createMono(() -> searchCommandBuilder.dropIndex(index, false));
	}

	@Override
	public Mono<String> dropindexDeleteDocs(K index) {
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
	public Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, CursorOptions cursor) {
		return createMono(() -> searchCommandBuilder.aggregate(index, query, cursor, null));
	}

	@Override
	public Mono<AggregateWithCursorResults<K>> aggregate(K index, V query, CursorOptions cursor,
			AggregateOptions<K, V> options) {
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
	public Mono<Long> sugaddIncr(K key, V string, double score) {
		return createMono(() -> searchCommandBuilder.sugaddIncr(key, string, score));
	}

	@Override
	public Mono<Long> sugadd(K key, V string, double score, V payload) {
		return createMono(() -> searchCommandBuilder.sugadd(key, string, score, payload));
	}

	@Override
	public Mono<Long> sugaddIncr(K key, V string, double score, V payload) {
		return createMono(() -> searchCommandBuilder.sugaddIncr(key, string, score, payload));
	}

	@Override
	public Mono<Long> sugadd(K key, Suggestion<V> suggestion) {
		return createMono(() -> searchCommandBuilder.sugadd(key, suggestion));
	}

	@Override
	public Mono<Long> sugaddIncr(K key, Suggestion<V> suggestion) {
		return createMono(() -> searchCommandBuilder.sugaddIncr(key, suggestion));
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
	public Mono<String> aliasadd(K name, K index) {
		return createMono(() -> searchCommandBuilder.aliasAdd(name, index));
	}

	@Override
	public Mono<String> aliasupdate(K name, K index) {
		return createMono(() -> searchCommandBuilder.aliasUpdate(name, index));
	}

	@Override
	public Mono<String> aliasdel(K name) {
		return createMono(() -> searchCommandBuilder.aliasDel(name));
	}

	@Override
	public Flux<K> list() {
		return createDissolvingFlux(searchCommandBuilder::list);
	}

	@Override
	public Flux<V> tagvals(K index, K field) {
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

	@Override
	public Mono<Long> jsonDel(K key) {
		return jsonDel(key, null);
	}

	@Override
	public Mono<Long> jsonDel(K key, K path) {
		return createMono(() -> jsonCommandBuilder.del(key, path));
	}

	@Override
	public Mono<V> jsonGet(K key, K... paths) {
		return jsonGet(key, null, paths);
	}

	@Override
	public Mono<V> jsonGet(K key, GetOptions options, K... paths) {
		return createMono(() -> jsonCommandBuilder.get(key, options, paths));
	}

	@Override
	public Flux<KeyValue<K, V>> jsonMget(K path, K... keys) {
		return createDissolvingFlux(() -> jsonCommandBuilder.mget(path, keys));
	}

	public Flux<KeyValue<K, V>> mget(K path, Iterable<K> keys) {
		return createDissolvingFlux(() -> jsonCommandBuilder.mgetKeyValue(path, keys));
	}

	@Override
	public Mono<String> jsonSet(K key, K path, V json) {
		return jsonSet(key, path, json, null);
	}

	@Override
	public Mono<String> jsonSet(K key, K path, V json, SetMode mode) {
		return createMono(() -> jsonCommandBuilder.set(key, path, json, mode));
	}

	@Override
	public Mono<String> jsonType(K key) {
		return jsonType(key, null);
	}

	@Override
	public Mono<String> jsonType(K key, K path) {
		return createMono(() -> jsonCommandBuilder.type(key, path));
	}

	@Override
	public Mono<V> numincrby(K key, K path, double number) {
		return createMono(() -> jsonCommandBuilder.numIncrBy(key, path, number));
	}

	@Override
	public Mono<V> nummultby(K key, K path, double number) {
		return createMono(() -> jsonCommandBuilder.numMultBy(key, path, number));
	}

	@Override
	public Mono<Long> strappend(K key, V json) {
		return strappend(key, null, json);
	}

	@Override
	public Mono<Long> strappend(K key, K path, V json) {
		return createMono(() -> jsonCommandBuilder.strAppend(key, path, json));
	}

	@Override
	public Mono<Long> strlen(K key, K path) {
		return createMono(() -> jsonCommandBuilder.strLen(key, path));
	}

	@Override
	public Mono<Long> arrappend(K key, K path, V... jsons) {
		return createMono(() -> jsonCommandBuilder.arrAppend(key, path, jsons));
	}

	@Override
	public Mono<Long> arrindex(K key, K path, V scalar) {
		return createMono(() -> jsonCommandBuilder.arrIndex(key, path, scalar, null, null));
	}

	@Override
	public Mono<Long> arrindex(K key, K path, V scalar, long start) {
		return createMono(() -> jsonCommandBuilder.arrIndex(key, path, scalar, start, null));
	}

	@Override
	public Mono<Long> arrindex(K key, K path, V scalar, long start, long stop) {
		return createMono(() -> jsonCommandBuilder.arrIndex(key, path, scalar, start, stop));
	}

	@Override
	public Mono<Long> arrinsert(K key, K path, long index, V... jsons) {
		return createMono(() -> jsonCommandBuilder.arrInsert(key, path, index, jsons));
	}

	@Override
	public Mono<Long> arrlen(K key) {
		return arrlen(key, null);
	}

	@Override
	public Mono<Long> arrlen(K key, K path) {
		return createMono(() -> jsonCommandBuilder.arrLen(key, path));
	}

	@Override
	public Mono<V> arrpop(K key) {
		return arrpop(key, null);
	}

	@Override
	public Mono<V> arrpop(K key, K path) {
		return createMono(() -> jsonCommandBuilder.arrPop(key, path, null));
	}

	@Override
	public Mono<V> arrpop(K key, K path, long index) {
		return createMono(() -> jsonCommandBuilder.arrPop(key, path, index));
	}

	@Override
	public Mono<Long> arrtrim(K key, K path, long start, long stop) {
		return createMono(() -> jsonCommandBuilder.arrTrim(key, path, start, stop));
	}

	@Override
	public Flux<K> objkeys(K key) {
		return objkeys(key, null);
	}

	@Override
	public Flux<K> objkeys(K key, K path) {
		return createDissolvingFlux(() -> jsonCommandBuilder.objKeys(key, path));
	}

	@Override
	public Mono<Long> objlen(K key) {
		return objlen(key, null);
	}

	@Override
	public Mono<Long> objlen(K key, K path) {
		return createMono(() -> jsonCommandBuilder.objLen(key, path));
	}

}
