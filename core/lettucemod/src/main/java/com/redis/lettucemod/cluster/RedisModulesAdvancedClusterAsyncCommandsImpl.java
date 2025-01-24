package com.redis.lettucemod.cluster;

import java.util.List;

import com.redis.lettucemod.RedisModulesAsyncCommandsImpl;
import com.redis.lettucemod.api.async.RedisModulesAsyncCommands;
import com.redis.lettucemod.bloom.BloomFilterInfo;
import com.redis.lettucemod.bloom.BloomFilterInfoType;
import com.redis.lettucemod.bloom.BloomFilterInsertOptions;
import com.redis.lettucemod.bloom.BloomFilterReserveOptions;
import com.redis.lettucemod.bloom.CmsInfo;
import com.redis.lettucemod.bloom.CuckooFilter;
import com.redis.lettucemod.bloom.CuckooFilterInsertOptions;
import com.redis.lettucemod.bloom.CuckooFilterReserveOptions;
import com.redis.lettucemod.bloom.LongScoredValue;
import com.redis.lettucemod.bloom.TDigestInfo;
import com.redis.lettucemod.bloom.TDigestMergeOptions;
import com.redis.lettucemod.bloom.TopKInfo;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.lettucemod.cluster.api.async.RedisModulesAdvancedClusterAsyncCommands;
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
import io.lettuce.core.Value;
import io.lettuce.core.cluster.MultiNodeExecution;
import io.lettuce.core.cluster.RedisAdvancedClusterAsyncCommandsImpl;
import io.lettuce.core.codec.RedisCodec;

@SuppressWarnings("unchecked")
public class RedisModulesAdvancedClusterAsyncCommandsImpl<K, V> extends RedisAdvancedClusterAsyncCommandsImpl<K, V>
		implements RedisModulesAdvancedClusterAsyncCommands<K, V> {

	private final RedisModulesAsyncCommandsImpl<K, V> delegate;

	public RedisModulesAdvancedClusterAsyncCommandsImpl(StatefulRedisModulesClusterConnection<K, V> connection,
			RedisCodec<K, V> codec) {
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
	public RedisFuture<List<V>> tsQueryIndex(V... filters) {
		return delegate.tsQueryIndex(filters);
	}

	@Override
	public RedisFuture<Long> tsDel(K key, TimeRange timeRange) {
		return delegate.tsDel(key, timeRange);
	}

	@Override
	public RedisFuture<Boolean> bfAdd(K key, V item) {
		return delegate.bfAdd(key, item);
	}

	@Override
	public RedisFuture<Long> bfCard(K key) {
		return delegate.bfCard(key);
	}

	@Override
	public RedisFuture<Boolean> bfExists(K key, V item) {
		return delegate.bfExists(key, item);
	}

	@Override
	public RedisFuture<BloomFilterInfo> bfInfo(K key) {
		return delegate.bfInfo(key);
	}

	@Override
	public RedisFuture<Long> bfInfo(K key, BloomFilterInfoType infoType) {
		return delegate.bfInfo(key, infoType);
	}

	@Override
	public RedisFuture<List<Boolean>> bfInsert(K key, V... items) {
		return delegate.bfInsert(key, items);
	}

	@Override
	public RedisFuture<List<Boolean>> bfInsert(K key, BloomFilterInsertOptions options, V... items) {
		return delegate.bfInsert(key, options, items);
	}

	@Override
	public RedisFuture<List<Boolean>> bfMAdd(K key, V... items) {
		return delegate.bfMAdd(key, items);
	}

	@Override
	public RedisFuture<List<Boolean>> bfMExists(K key, V... items) {
		return delegate.bfMExists(key, items);
	}

	@Override
	public RedisFuture<String> bfReserve(K key, double errorRate, long capacity) {
		return delegate.bfReserve(key, errorRate, capacity);
	}

	@Override
	public RedisFuture<String> bfReserve(K key, double errorRate, long capacity, BloomFilterReserveOptions options) {
		return delegate.bfReserve(key, errorRate, capacity, options);
	}

	@Override
	public RedisFuture<Boolean> cfAdd(K key, V item) {
		return delegate.cfAdd(key, item);
	}

	@Override
	public RedisFuture<Boolean> cfAddNx(K key, V item) {
		return delegate.cfAddNx(key, item);
	}

	@Override
	public RedisFuture<Long> cfCount(K key, V item) {
		return delegate.cfCount(key, item);
	}

	@Override
	public RedisFuture<Boolean> cfDel(K key, V item) {
		return delegate.cfDel(key, item);
	}

	@Override
	public RedisFuture<Boolean> cfExists(K key, V item) {
		return delegate.cfExists(key, item);
	}

	@Override
	public RedisFuture<CuckooFilter> cfInfo(K key) {
		return delegate.cfInfo(key);
	}

	@Override
	public RedisFuture<List<Long>> cfInsert(K key, V... items) {
		return delegate.cfInsert(key, items);
	}

	@Override
	public RedisFuture<List<Long>> cfInsert(K key, CuckooFilterInsertOptions options, V... items) {
		return delegate.cfInsert(key, options, items);
	}

	@Override
	public RedisFuture<List<Long>> cfInsertNx(K key, V... items) {
		return delegate.cfInsertNx(key, items);
	}

	@Override
	public RedisFuture<List<Long>> cfInsertNx(K key, CuckooFilterInsertOptions options, V... items) {
		return delegate.cfInsertNx(key, options, items);
	}

	@Override
	public RedisFuture<List<Boolean>> cfMExists(K key, V... items) {
		return delegate.cfMExists(key, items);
	}

	@Override
	public RedisFuture<String> cfReserve(K key, long capacity) {
		return delegate.cfReserve(key, capacity);
	}

	@Override
	public RedisFuture<String> cfReserve(K key, long capacity, CuckooFilterReserveOptions options) {
		return delegate.cfReserve(key, capacity, options);
	}

	@Override
	public RedisFuture<Long> cmsIncrBy(K key, V item, long increment) {
		return delegate.cmsIncrBy(key, item, increment);
	}

	@Override
	public RedisFuture<List<Long>> cmsIncrBy(K key, LongScoredValue<V>... itemIncrements) {
		return delegate.cmsIncrBy(key, itemIncrements);
	}

	@Override
	public RedisFuture<String> cmsInitByProb(K key, double error, double probability) {
		return delegate.cmsInitByProb(key, error, probability);
	}

	@Override
	public RedisFuture<String> cmsInitByDim(K key, long width, long depth) {
		return delegate.cmsInitByDim(key, width, depth);
	}

	@Override
	public RedisFuture<List<Long>> cmsQuery(K key, V... items) {
		return delegate.cmsQuery(key, items);
	}

	@Override
	public RedisFuture<String> cmsMerge(K destKey, K... keys) {
		return delegate.cmsMerge(destKey, keys);
	}

	@Override
	public RedisFuture<String> cmsMerge(K destKey, LongScoredValue<K>... sourceKeyWeights) {
		return delegate.cmsMerge(destKey, sourceKeyWeights);
	}

	@Override
	public RedisFuture<CmsInfo> cmsInfo(K key) {
		return delegate.cmsInfo(key);
	}

	@Override
	public RedisFuture<List<Value<V>>> topKAdd(K key, V... items) {
		return delegate.topKAdd(key, items);
	}

	@Override
	public RedisFuture<List<Value<V>>> topKIncrBy(K key, LongScoredValue<V>... itemIncrements) {
		return delegate.topKIncrBy(key, itemIncrements);
	}

	@Override
	public RedisFuture<TopKInfo> topKInfo(K key) {
		return delegate.topKInfo(key);
	}

	@Override
	public RedisFuture<List<String>> topKList(K key) {
		return delegate.topKList(key);
	}

	@Override
	public RedisFuture<List<KeyValue<String, Long>>> topKListWithScores(K key) {
		return delegate.topKListWithScores(key);
	}

	@Override
	public RedisFuture<List<Boolean>> topKQuery(K key, V... items) {
		return delegate.topKQuery(key, items);
	}

	@Override
	public RedisFuture<String> topKReserve(K key, long k) {
		return delegate.topKReserve(key, k);
	}

	@Override
	public RedisFuture<String> topKReserve(K key, long k, long width, long depth, double decay) {
		return delegate.topKReserve(key, k, width, depth, decay);
	}

	@Override
	public RedisFuture<String> tDigestAdd(K key, double... value) {
		return delegate.tDigestAdd(key, value);
	}

	@Override
	public RedisFuture<List<Double>> tDigestByRank(K key, long... ranks) {
		return delegate.tDigestByRank(key, ranks);
	}

	@Override
	public RedisFuture<List<Double>> tDigestByRevRank(K key, long... revRanks) {
		return delegate.tDigestByRevRank(key, revRanks);
	}

	@Override
	public RedisFuture<List<Double>> tDigestCdf(K key, double... values) {
		return delegate.tDigestCdf(key, values);
	}

	@Override
	public RedisFuture<String> tDigestCreate(K key) {
		return delegate.tDigestCreate(key);
	}

	@Override
	public RedisFuture<String> tDigestCreate(K key, long compression) {
		return delegate.tDigestCreate(key, compression);
	}

	@Override
	public RedisFuture<TDigestInfo> tDigestInfo(K key) {
		return delegate.tDigestInfo(key);
	}

	@Override
	public RedisFuture<Double> tDigestMax(K key) {
		return delegate.tDigestMax(key);
	}

	@Override
	public RedisFuture<String> tDigestMerge(K destinationKey, K... sourceKeys) {
		return delegate.tDigestMerge(destinationKey, sourceKeys);
	}

	@Override
	public RedisFuture<String> tDigestMerge(K destinationKey, TDigestMergeOptions options, K... sourceKeys) {
		return delegate.tDigestMerge(destinationKey, options, sourceKeys);
	}

	@Override
	public RedisFuture<Double> tDigestMin(K key) {
		return delegate.tDigestMin(key);
	}

	@Override
	public RedisFuture<List<Double>> tDigestQuantile(K key, double... quantiles) {
		return delegate.tDigestQuantile(key, quantiles);
	}

	@Override
	public RedisFuture<List<Long>> tDigestRank(K key, double... values) {
		return delegate.tDigestRank(key, values);
	}

	@Override
	public RedisFuture<String> tDigestReset(K key) {
		return delegate.tDigestReset(key);
	}

	@Override
	public RedisFuture<List<Long>> tDigestRevRank(K key, double... values) {
		return delegate.tDigestRevRank(key, values);
	}

	@Override
	public RedisFuture<Double> tDigestTrimmedMean(K key, double lowCutQuantile, double highCutQuantile) {
		return delegate.tDigestTrimmedMean(key, lowCutQuantile, highCutQuantile);
	}
}
