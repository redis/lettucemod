package com.redis.lettucemod;

import java.util.List;
import java.util.Map;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.bloom.BloomCommandBuilder;
import com.redis.lettucemod.bloom.BloomFilterInfo;
import com.redis.lettucemod.bloom.BloomFilterInfoType;
import com.redis.lettucemod.bloom.BloomFilterInsertOptions;
import com.redis.lettucemod.bloom.BloomFilterReserveOptions;
import com.redis.lettucemod.bloom.CuckooFilter;
import com.redis.lettucemod.bloom.CuckooFilterInsertOptions;
import com.redis.lettucemod.bloom.CuckooFilterReserveOptions;
import com.redis.lettucemod.bloom.TDigestInfo;
import com.redis.lettucemod.bloom.TDigestMergeOptions;
import com.redis.lettucemod.bloom.TopKInfo;
import com.redis.lettucemod.cms.CmsInfo;
import com.redis.lettucemod.gears.Execution;
import com.redis.lettucemod.gears.ExecutionDetails;
import com.redis.lettucemod.gears.ExecutionMode;
import com.redis.lettucemod.gears.GearsCommandBuilder;
import com.redis.lettucemod.gears.Registration;
import com.redis.lettucemod.json.ArrpopOptions;
import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.JSONCommandBuilder;
import com.redis.lettucemod.json.SetMode;
import com.redis.lettucemod.json.Slice;
import com.redis.lettucemod.output.ExecutionResults;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.AggregateResults;
import com.redis.lettucemod.search.AggregateWithCursorResults;
import com.redis.lettucemod.search.CursorOptions;
import com.redis.lettucemod.search.Field;
import com.redis.lettucemod.search.SearchCommandBuilder;
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
import com.redis.lettucemod.timeseries.TimeSeriesCommandBuilder;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisAsyncCommandsImpl;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.Value;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.KeyValueStreamingChannel;

@SuppressWarnings("unchecked")
public class RedisModulesAsyncCommandsImpl<K, V> extends RedisAsyncCommandsImpl<K, V>
		implements RedisModulesAsyncCommands<K, V> {

	private final GearsCommandBuilder<K, V> gearsCommandBuilder;
	private final TimeSeriesCommandBuilder<K, V> timeSeriesCommandBuilder;
	private final SearchCommandBuilder<K, V> searchCommandBuilder;
	private final JSONCommandBuilder<K, V> jsonCommandBuilder;
	private final BloomCommandBuilder<K, V> bloomCommandBuilder;

	public RedisModulesAsyncCommandsImpl(StatefulRedisModulesConnection<K, V> connection, RedisCodec<K, V> codec) {
		super(connection, codec);
		this.gearsCommandBuilder = new GearsCommandBuilder<>(codec);
		this.timeSeriesCommandBuilder = new TimeSeriesCommandBuilder<>(codec);
		this.searchCommandBuilder = new SearchCommandBuilder<>(codec);
		this.jsonCommandBuilder = new JSONCommandBuilder<>(codec);
		this.bloomCommandBuilder = new BloomCommandBuilder<>(codec);
	}

	@Override
	public StatefulRedisModulesConnection<K, V> getStatefulConnection() {
		return (StatefulRedisModulesConnection<K, V>) super.getStatefulConnection();
	}

	@Override
	public RedisFuture<String> rgAbortexecution(String id) {
		return dispatch(gearsCommandBuilder.abortExecution(id));
	}

	@Override
	public RedisFuture<List<V>> rgConfigget(K... keys) {
		return dispatch(gearsCommandBuilder.configGet(keys));
	}

	@Override
	public RedisFuture<List<V>> rgConfigset(Map<K, V> map) {
		return dispatch(gearsCommandBuilder.configSet(map));
	}

	@Override
	public RedisFuture<String> rgDropexecution(String id) {
		return dispatch(gearsCommandBuilder.dropExecution(id));
	}

	@Override
	public RedisFuture<List<Execution>> rgDumpexecutions() {
		return dispatch(gearsCommandBuilder.dumpExecutions());
	}

	@Override
	public RedisFuture<ExecutionResults> rgPyexecute(String function, V... requirements) {
		return dispatch(gearsCommandBuilder.pyExecute(function, requirements));
	}

	@Override
	public RedisFuture<String> rgPyexecuteUnblocking(String function, V... requirements) {
		return dispatch(gearsCommandBuilder.pyExecuteUnblocking(function, requirements));
	}

	@Override
	public RedisFuture<List<Object>> rgTrigger(String trigger, V... args) {
		return dispatch(gearsCommandBuilder.trigger(trigger, args));
	}

	@Override
	public RedisFuture<String> rgUnregister(String id) {
		return dispatch(gearsCommandBuilder.unregister(id));
	}

	@Override
	public RedisFuture<List<Registration>> rgDumpregistrations() {
		return dispatch(gearsCommandBuilder.dumpRegistrations());
	}

	@Override
	public RedisFuture<ExecutionDetails> rgGetexecution(String id) {
		return dispatch(gearsCommandBuilder.getExecution(id));
	}

	@Override
	public RedisFuture<ExecutionDetails> rgGetexecution(String id, ExecutionMode mode) {
		return dispatch(gearsCommandBuilder.getExecution(id, mode));
	}

	@Override
	public RedisFuture<ExecutionResults> rgGetresults(String id) {
		return dispatch(gearsCommandBuilder.getResults(id));
	}

	@Override
	public RedisFuture<ExecutionResults> rgGetresultsblocking(String id) {
		return dispatch(gearsCommandBuilder.getResultsBlocking(id));
	}

	@Override
	public RedisFuture<String> tsCreate(K key, CreateOptions<K, V> options) {
		return dispatch(timeSeriesCommandBuilder.create(key, options));
	}

	@Override
	public RedisFuture<String> tsAlter(K key, AlterOptions<K, V> options) {
		return dispatch(timeSeriesCommandBuilder.alter(key, options));
	}

	@Override
	public RedisFuture<Long> tsAdd(K key, Sample sample) {
		return dispatch(timeSeriesCommandBuilder.add(key, sample));
	}

	@Override
	public RedisFuture<Long> tsAdd(K key, Sample sample, AddOptions<K, V> options) {
		return dispatch(timeSeriesCommandBuilder.add(key, sample, options));
	}

	@Override
	public RedisFuture<Long> tsDecrby(K key, double value) {
		return dispatch(timeSeriesCommandBuilder.decrby(key, value, null));
	}

	@Override
	public RedisFuture<Long> tsDecrby(K key, double value, IncrbyOptions<K, V> options) {
		return dispatch(timeSeriesCommandBuilder.decrby(key, value, options));
	}

	@Override
	public RedisFuture<Long> tsIncrby(K key, double value) {
		return dispatch(timeSeriesCommandBuilder.incrby(key, value, null));
	}

	@Override
	public RedisFuture<Long> tsIncrby(K key, double value, IncrbyOptions<K, V> options) {
		return dispatch(timeSeriesCommandBuilder.incrby(key, value, options));
	}

	@Override
	public RedisFuture<List<Long>> tsMadd(KeySample<K>... samples) {
		return dispatch(timeSeriesCommandBuilder.madd(samples));
	}

	@Override
	public RedisFuture<String> tsCreaterule(K sourceKey, K destKey, CreateRuleOptions options) {
		return dispatch(timeSeriesCommandBuilder.createRule(sourceKey, destKey, options));
	}

	@Override
	public RedisFuture<String> tsDeleterule(K sourceKey, K destKey) {
		return dispatch(timeSeriesCommandBuilder.deleteRule(sourceKey, destKey));
	}

	@Override
	public RedisFuture<List<Sample>> tsRange(K key, TimeRange range) {
		return dispatch(timeSeriesCommandBuilder.range(key, range));
	}

	@Override
	public RedisFuture<List<Sample>> tsRange(K key, TimeRange range, RangeOptions options) {
		return dispatch(timeSeriesCommandBuilder.range(key, range, options));
	}

	@Override
	public RedisFuture<List<Sample>> tsRevrange(K key, TimeRange range) {
		return dispatch(timeSeriesCommandBuilder.revrange(key, range));
	}

	@Override
	public RedisFuture<List<Sample>> tsRevrange(K key, TimeRange range, RangeOptions options) {
		return dispatch(timeSeriesCommandBuilder.revrange(key, range, options));
	}

	@Override
	public RedisFuture<List<RangeResult<K, V>>> tsMrange(TimeRange range) {
		return dispatch(timeSeriesCommandBuilder.mrange(range));
	}

	@Override
	public RedisFuture<List<RangeResult<K, V>>> tsMrange(TimeRange range, MRangeOptions<K, V> options) {
		return dispatch(timeSeriesCommandBuilder.mrange(range, options));
	}

	@Override
	public RedisFuture<List<RangeResult<K, V>>> tsMrevrange(TimeRange range) {
		return dispatch(timeSeriesCommandBuilder.mrevrange(range));
	}

	@Override
	public RedisFuture<List<RangeResult<K, V>>> tsMrevrange(TimeRange range, MRangeOptions<K, V> options) {
		return dispatch(timeSeriesCommandBuilder.mrevrange(range, options));
	}

	@Override
	public RedisFuture<Sample> tsGet(K key) {
		return dispatch(timeSeriesCommandBuilder.get(key));
	}

	@Override
	public RedisFuture<List<GetResult<K, V>>> tsMget(MGetOptions<K, V> options) {
		return dispatch(timeSeriesCommandBuilder.mget(options));
	}

	@Override
	public RedisFuture<List<GetResult<K, V>>> tsMget(V... filters) {
		return dispatch(timeSeriesCommandBuilder.mget(filters));
	}

	@Override
	public RedisFuture<List<GetResult<K, V>>> tsMgetWithLabels(V... filters) {
		return dispatch(timeSeriesCommandBuilder.mgetWithLabels(filters));
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
	public RedisFuture<List<V>> tsQueryIndex(V... filters) {
		return dispatch(timeSeriesCommandBuilder.queryIndex(filters));
	}

	@Override
	public RedisFuture<Long> tsDel(K key, TimeRange timeRange) {
		return dispatch(timeSeriesCommandBuilder.tsDel(key, timeRange));
	}

	@Override
	public RedisFuture<String> ftCreate(K index, Field<K>... fields) {
		return ftCreate(index, null, fields);
	}

	@Override
	public RedisFuture<String> ftCreate(K index, com.redis.lettucemod.search.CreateOptions<K, V> options,
			Field<K>... fields) {
		return dispatch(searchCommandBuilder.create(index, options, fields));
	}

	@Override
	public RedisFuture<String> ftDropindex(K index) {
		return dispatch(searchCommandBuilder.dropIndex(index, false));
	}

	@Override
	public RedisFuture<String> ftDropindexDeleteDocs(K index) {
		return dispatch(searchCommandBuilder.dropIndex(index, true));
	}

	@Override
	public RedisFuture<List<Object>> ftInfo(K index) {
		return dispatch(searchCommandBuilder.info(index));
	}

	@Override
	public RedisFuture<SearchResults<K, V>> ftSearch(K index, V query) {
		return ftSearch(index, query, null);
	}

	@Override
	public RedisFuture<SearchResults<K, V>> ftSearch(K index, V query, SearchOptions<K, V> options) {
		return dispatch(searchCommandBuilder.search(index, query, options));
	}

	@Override
	public RedisFuture<AggregateResults<K>> ftAggregate(K index, V query) {
		return ftAggregate(index, query, (AggregateOptions<K, V>) null);
	}

	@Override
	public RedisFuture<AggregateResults<K>> ftAggregate(K index, V query, AggregateOptions<K, V> options) {
		return dispatch(searchCommandBuilder.aggregate(index, query, options));
	}

	@Override
	public RedisFuture<AggregateWithCursorResults<K>> ftAggregate(K index, V query, CursorOptions cursor) {
		return ftAggregate(index, query, cursor, null);
	}

	@Override
	public RedisFuture<AggregateWithCursorResults<K>> ftAggregate(K index, V query, CursorOptions cursor,
			AggregateOptions<K, V> options) {
		return dispatch(searchCommandBuilder.aggregate(index, query, cursor, options));
	}

	@Override
	public RedisFuture<AggregateWithCursorResults<K>> ftCursorRead(K index, long cursor) {
		return dispatch(searchCommandBuilder.cursorRead(index, cursor, null));
	}

	@Override
	public RedisFuture<AggregateWithCursorResults<K>> ftCursorRead(K index, long cursor, long count) {
		return dispatch(searchCommandBuilder.cursorRead(index, cursor, count));
	}

	@Override
	public RedisFuture<String> ftCursorDelete(K index, long cursor) {
		return dispatch(searchCommandBuilder.cursorDelete(index, cursor));
	}

	@Override
	public RedisFuture<Long> ftSugadd(K key, Suggestion<V> suggestion) {
		return dispatch(searchCommandBuilder.sugadd(key, suggestion));
	}

	@Override
	public RedisFuture<Long> ftSugaddIncr(K key, Suggestion<V> suggestion) {
		return dispatch(searchCommandBuilder.sugaddIncr(key, suggestion));
	}

	@Override
	public RedisFuture<List<Suggestion<V>>> ftSugget(K key, V prefix) {
		return dispatch(searchCommandBuilder.sugget(key, prefix));
	}

	@Override
	public RedisFuture<List<Suggestion<V>>> ftSugget(K key, V prefix, SuggetOptions options) {
		return dispatch(searchCommandBuilder.sugget(key, prefix, options));
	}

	@Override
	public RedisFuture<Boolean> ftSugdel(K key, V string) {
		return dispatch(searchCommandBuilder.sugdel(key, string));
	}

	@Override
	public RedisFuture<Long> ftSuglen(K key) {
		return dispatch(searchCommandBuilder.suglen(key));
	}

	@Override
	public RedisFuture<String> ftAlter(K index, Field<K> field) {
		return dispatch(searchCommandBuilder.alter(index, field));
	}

	@Override
	public RedisFuture<String> ftAliasadd(K name, K index) {
		return dispatch(searchCommandBuilder.aliasAdd(name, index));
	}

	@Override
	public RedisFuture<String> ftAliasdel(K name) {
		return dispatch(searchCommandBuilder.aliasDel(name));
	}

	@Override
	public RedisFuture<String> ftAliasupdate(K name, K index) {
		return dispatch(searchCommandBuilder.aliasUpdate(name, index));
	}

	@Override
	public RedisFuture<List<K>> ftList() {
		return dispatch(searchCommandBuilder.list());
	}

	@Override
	public RedisFuture<List<V>> ftTagvals(K index, K field) {
		return dispatch(searchCommandBuilder.tagVals(index, field));
	}

	@Override
	public RedisFuture<Long> ftDictadd(K dict, V... terms) {
		return dispatch(searchCommandBuilder.dictadd(dict, terms));
	}

	@Override
	public RedisFuture<Long> ftDictdel(K dict, V... terms) {
		return dispatch(searchCommandBuilder.dictdel(dict, terms));
	}

	@Override
	public RedisFuture<List<V>> ftDictdump(K dict) {
		return dispatch(searchCommandBuilder.dictdump(dict));
	}

	@Override
	public RedisFuture<Long> jsonDel(K key) {
		return jsonDel(key, null);
	}

	@Override
	public RedisFuture<Long> jsonDel(K key, String path) {
		return dispatch(jsonCommandBuilder.del(key, path));
	}

	@Override
	public RedisFuture<V> jsonGet(K key, K... paths) {
		return jsonGet(key, null, paths);
	}

	@Override
	public RedisFuture<V> jsonGet(K key, GetOptions options, K... paths) {
		return dispatch(jsonCommandBuilder.get(key, options, paths));
	}

	@Override
	public RedisFuture<List<KeyValue<K, V>>> jsonMget(String path, K... keys) {
		return dispatch(jsonCommandBuilder.mgetKeyValue(path, keys));
	}

	public RedisFuture<List<KeyValue<K, V>>> jsonMget(String path, Iterable<K> keys) {
		return dispatch(jsonCommandBuilder.mgetKeyValue(path, keys));
	}

	@Override
	public RedisFuture<Long> jsonMget(KeyValueStreamingChannel<K, V> channel, String path, K... keys) {
		return dispatch(jsonCommandBuilder.mget(channel, path, keys));
	}

	public RedisFuture<Long> jsonMget(KeyValueStreamingChannel<K, V> channel, String path, Iterable<K> keys) {
		return dispatch(jsonCommandBuilder.mget(channel, path, keys));
	}

	@Override
	public RedisFuture<String> jsonSet(K key, String path, V json) {
		return jsonSet(key, path, json, null);
	}

	@Override
	public RedisFuture<String> jsonSet(K key, String path, V json, SetMode options) {
		return dispatch(jsonCommandBuilder.set(key, path, json, options));
	}

	@Override
	public RedisFuture<String> jsonMerge(K key, String path, V json) {
		return dispatch(jsonCommandBuilder.merge(key, path, json));
	}

	@Override
	public RedisFuture<String> jsonType(K key) {
		return jsonType(key, null);
	}

	@Override
	public RedisFuture<String> jsonType(K key, String path) {
		return dispatch(jsonCommandBuilder.type(key, path));
	}

	@Override
	public RedisFuture<V> jsonNumincrby(K key, String path, double number) {
		return dispatch(jsonCommandBuilder.numIncrBy(key, path, number));
	}

	@Override
	public RedisFuture<V> jsonNummultby(K key, String path, double number) {
		return dispatch(jsonCommandBuilder.numMultBy(key, path, number));
	}

	@Override
	public RedisFuture<Long> jsonStrappend(K key, V json) {
		return jsonStrappend(key, null, json);
	}

	@Override
	public RedisFuture<Long> jsonStrappend(K key, String path, V json) {
		return dispatch(jsonCommandBuilder.strAppend(key, path, json));
	}

	@Override
	public RedisFuture<Long> jsonStrlen(K key, String path) {
		return dispatch(jsonCommandBuilder.strLen(key, path));
	}

	@Override
	public RedisFuture<Long> jsonArrappend(K key, String path, V... jsons) {
		return dispatch(jsonCommandBuilder.arrAppend(key, path, jsons));
	}

	@Override
	public RedisFuture<Long> jsonArrindex(K key, String path, V scalar) {
		return jsonArrindex(key, path, scalar, null);
	}

	@Override
	public RedisFuture<Long> jsonArrindex(K key, String path, V scalar, Slice slice) {
		return dispatch(jsonCommandBuilder.arrIndex(key, path, scalar, slice));
	}

	@Override
	public RedisFuture<Long> jsonArrinsert(K key, String path, long index, V... jsons) {
		return dispatch(jsonCommandBuilder.arrInsert(key, path, index, jsons));
	}

	@Override
	public RedisFuture<Long> jsonArrlen(K key) {
		return jsonArrlen(key, null);
	}

	@Override
	public RedisFuture<Long> jsonArrlen(K key, String path) {
		return dispatch(jsonCommandBuilder.arrLen(key, path));
	}

	@Override
	public RedisFuture<V> jsonArrpop(K key) {
		return jsonArrpop(key, null);
	}

	@Override
	public RedisFuture<V> jsonArrpop(K key, ArrpopOptions<K> options) {
		return dispatch(jsonCommandBuilder.arrPop(key, options));
	}

	@Override
	public RedisFuture<Long> jsonArrtrim(K key, String path, long start, long stop) {
		return dispatch(jsonCommandBuilder.arrTrim(key, path, start, stop));
	}

	@Override
	public RedisFuture<List<K>> jsonObjkeys(K key) {
		return jsonObjkeys(key, null);
	}

	@Override
	public RedisFuture<List<K>> jsonObjkeys(K key, String path) {
		return dispatch(jsonCommandBuilder.objKeys(key, path));
	}

	@Override
	public RedisFuture<Long> jsonObjlen(K key) {
		return jsonObjlen(key, null);
	}

	@Override
	public RedisFuture<Long> jsonObjlen(K key, String path) {
		return dispatch(jsonCommandBuilder.objLen(key, path));
	}

	@Override
	public RedisFuture<Boolean> bfAdd(K key, V item) {
		return dispatch(bloomCommandBuilder.bfAdd(key, item));
	}

	@Override
	public RedisFuture<Long> bfCard(K key) {
		return dispatch(bloomCommandBuilder.bfCard(key));
	}

	@Override
	public RedisFuture<Boolean> bfExists(K key, V item) {
		return dispatch(bloomCommandBuilder.bfExists(key, item));
	}

	@Override
	public RedisFuture<BloomFilterInfo> bfInfo(K key) {
		return dispatch(bloomCommandBuilder.bfInfo(key));
	}

	@Override
	public RedisFuture<Long> bfInfo(K key, BloomFilterInfoType type) {
		return dispatch(bloomCommandBuilder.bfInfo(key, type));
	}

	@Override
	public RedisFuture<List<Boolean>> bfInsert(K key, V... items) {
		return dispatch(bloomCommandBuilder.bfInsert(key, items));
	}

	@Override
	public RedisFuture<List<Boolean>> bfInsert(K key, BloomFilterInsertOptions options, V... items) {
		return dispatch(bloomCommandBuilder.bfInsert(key, options, items));
	}

	@Override
	public RedisFuture<List<Boolean>> bfMAdd(K key, V... items) {
		return dispatch(bloomCommandBuilder.bfMAdd(key, items));
	}

	@Override
	public RedisFuture<List<Boolean>> bfMExists(K key, V... items) {
		return dispatch(bloomCommandBuilder.bfMExists(key, items));
	}

	@Override
	public RedisFuture<String> bfReserve(K key, double errorRate, long capacity) {
		return bfReserve(key, errorRate, capacity, null);
	}

	@Override
	public RedisFuture<String> bfReserve(K key, double errorRate, long capacity, BloomFilterReserveOptions options) {
		return dispatch(bloomCommandBuilder.bfReserve(key, errorRate, capacity, options));
	}

	@Override
	public RedisFuture<Boolean> cfAdd(K key, V item) {
		return dispatch(bloomCommandBuilder.cfAdd(key, item));
	}

	@Override
	public RedisFuture<Boolean> cfAddNx(K key, V item) {
		return dispatch(bloomCommandBuilder.cfAddNx(key, item));
	}

	@Override
	public RedisFuture<Long> cfCount(K key, V item) {
		return dispatch(bloomCommandBuilder.cfCount(key, item));
	}

	@Override
	public RedisFuture<Boolean> cfDel(K key, V item) {
		return dispatch(bloomCommandBuilder.cfDel(key, item));
	}

	@Override
	public RedisFuture<Boolean> cfExists(K key, V item) {
		return dispatch(bloomCommandBuilder.cfExists(key, item));
	}

	@Override
	public RedisFuture<CuckooFilter> cfInfo(K key) {
		return dispatch(bloomCommandBuilder.cfInfo(key));
	}

	@Override
	public RedisFuture<List<Long>> cfInsert(K key, V... items) {
		return dispatch(bloomCommandBuilder.cfInsert(key, items));
	}

	@Override
	public RedisFuture<List<Long>> cfInsert(K key, CuckooFilterInsertOptions options, V... items) {
		return dispatch(bloomCommandBuilder.cfInsert(key, items, options));
	}

	@Override
	public RedisFuture<List<Long>> cfInsertNx(K key, V... items) {
		return dispatch(bloomCommandBuilder.cfInsertNx(key, items));
	}

	@Override
	public RedisFuture<List<Long>> cfInsertNx(K key, CuckooFilterInsertOptions options, V... items) {
		return dispatch(bloomCommandBuilder.cfInsertNx(key, items, options));
	}

	@Override
	public RedisFuture<List<Boolean>> cfMExists(K key, V... items) {
		return dispatch(bloomCommandBuilder.cfMExists(key, items));
	}

	@Override
	public RedisFuture<String> cfReserve(K key, long capacity) {
		return cfReserve(key, capacity, null);
	}

	@Override
	public RedisFuture<String> cfReserve(K key, long capacity, CuckooFilterReserveOptions options) {
		return dispatch(bloomCommandBuilder.cfReserve(key, capacity, options));
	}

	@Override
	public RedisFuture<Long> cmsIncrBy(K key, V item, long increment) {
		return dispatch(bloomCommandBuilder.cmsIncrBy(key, item, increment));
	}

	@Override
	public RedisFuture<List<Long>> cmsIncrBy(K key, Map<V, Long> increments) {
		return dispatch(bloomCommandBuilder.cmsIncrBy(key, increments));
	}

	@Override
	public RedisFuture<String> cmsInitByProb(K key, double error, double probability) {
		return dispatch(bloomCommandBuilder.cmsInitByProb(key, error, probability));
	}

	@Override
	public RedisFuture<String> cmsInitByDim(K key, long width, long depth) {
		return dispatch(bloomCommandBuilder.cmsInitByDim(key, width, depth));
	}

	@Override
	public RedisFuture<List<Long>> cmsQuery(K key, V... items) {
		return dispatch(bloomCommandBuilder.cmsQuery(key, items));
	}

	@Override
	public RedisFuture<String> cmsMerge(K destKey, K... keys) {
		return dispatch(bloomCommandBuilder.cmsMerge(destKey, keys));
	}

	@Override
	public RedisFuture<String> cmsMerge(K destKey, Map<K, Long> keyWeightMap) {
		return dispatch(bloomCommandBuilder.cmsMerge(destKey, keyWeightMap));
	}

	@Override
	public RedisFuture<CmsInfo> cmsInfo(K key) {
		return dispatch(bloomCommandBuilder.cmsInfo(key));
	}

	@Override
	public RedisFuture<List<Value<V>>> topKAdd(K key, V... items) {
		return dispatch(bloomCommandBuilder.topKAdd(key, items));
	}

	@Override
	public RedisFuture<List<Value<V>>> topKIncrBy(K key, Map<V, Long> increments) {
		return dispatch(bloomCommandBuilder.topKIncrBy(key, increments));
	}

	@Override
	public RedisFuture<TopKInfo> topKInfo(K key) {
		return dispatch(bloomCommandBuilder.topKInfo(key));
	}

	@Override
	public RedisFuture<List<String>> topKList(K key) {
		return dispatch(bloomCommandBuilder.topKList(key));
	}

	@Override
	public RedisFuture<List<KeyValue<String, Long>>> topKListWithScores(K key) {
		return dispatch(bloomCommandBuilder.topKListWithScores(key));
	}

	@Override
	public RedisFuture<List<Boolean>> topKQuery(K key, V... items) {
		return dispatch(bloomCommandBuilder.topKQuery(key, items));
	}

	@Override
	public RedisFuture<String> topKReserve(K key, long k) {
		return dispatch(bloomCommandBuilder.topKReserve(key, k));
	}

	@Override
	public RedisFuture<String> topKReserve(K key, long k, long width, long depth, double decay) {
		return dispatch(bloomCommandBuilder.topKReserve(key, k, width, depth, decay));
	}

	@Override
	public RedisFuture<String> tDigestAdd(K key, double... values) {
		return dispatch(bloomCommandBuilder.tDigestAdd(key, values));
	}

	@Override
	public RedisFuture<List<Double>> tDigestByRank(K key, long... ranks) {
		return dispatch(bloomCommandBuilder.tDigestByRank(key, ranks));
	}

	@Override
	public RedisFuture<List<Double>> tDigestByRevRank(K key, long... revRanks) {
		return dispatch(bloomCommandBuilder.tDigestByRevRank(key, revRanks));
	}

	@Override
	public RedisFuture<List<Double>> tDigestCdf(K key, double... values) {
		return dispatch(bloomCommandBuilder.tDigestCdf(key, values));
	}

	@Override
	public RedisFuture<String> tDigestCreate(K key) {
		return dispatch(bloomCommandBuilder.tDigestCreate(key));
	}

	@Override
	public RedisFuture<String> tDigestCreate(K key, long compression) {
		return dispatch(bloomCommandBuilder.tDigestCreate(key, compression));
	}

	@Override
	public RedisFuture<TDigestInfo> tDigestInfo(K key) {
		return dispatch(bloomCommandBuilder.tDigestInfo(key));
	}

	@Override
	public RedisFuture<Double> tDigestMax(K key) {
		return dispatch(bloomCommandBuilder.tDigestMax(key));
	}

	@Override
	public RedisFuture<String> tDigestMerge(K destinationKey, K... sourceKeys) {
		return dispatch(bloomCommandBuilder.tDigestMerge(destinationKey, sourceKeys));
	}

	@Override
	public RedisFuture<String> tDigestMerge(K destinationKey, TDigestMergeOptions options, K... sourceKeys) {
		return dispatch(bloomCommandBuilder.tDigestMerge(destinationKey, options, sourceKeys));
	}

	@Override
	public RedisFuture<Double> tDigestMin(K key) {
		return dispatch(bloomCommandBuilder.tDigestMin(key));
	}

	@Override
	public RedisFuture<List<Double>> tDigestQuantile(K key, double... quantiles) {
		return dispatch(bloomCommandBuilder.tDigestQuantile(key, quantiles));
	}

	@Override
	public RedisFuture<List<Long>> tDigestRank(K key, double... values) {
		return dispatch(bloomCommandBuilder.tDigestRank(key, values));
	}

	@Override
	public RedisFuture<String> tDigestReset(K key) {
		return dispatch(bloomCommandBuilder.tDigestReset(key));
	}

	@Override
	public RedisFuture<List<Long>> tDigestRevRank(K key, double... values) {
		return dispatch(bloomCommandBuilder.tDigestRevRank(key, values));
	}

	@Override
	public RedisFuture<Double> tDigestTrimmedMean(K key, double lowCutQuantile, double highCutQuantile) {
		return dispatch(bloomCommandBuilder.tDigestTrimmedMean(key, lowCutQuantile, highCutQuantile));
	}
}
