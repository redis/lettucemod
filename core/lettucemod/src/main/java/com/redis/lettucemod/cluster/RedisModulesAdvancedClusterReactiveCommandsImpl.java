package com.redis.lettucemod.cluster;

import com.redis.lettucemod.RedisModulesReactiveCommandsImpl;
import com.redis.lettucemod.bloom.*;
import com.redis.lettucemod.cluster.api.StatefulRedisModulesClusterConnection;
import com.redis.lettucemod.cluster.api.reactive.RedisModulesAdvancedClusterReactiveCommands;
import com.redis.lettucemod.timeseries.*;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Value;
import io.lettuce.core.cluster.RedisAdvancedClusterReactiveCommandsImpl;
import io.lettuce.core.codec.RedisCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
public class RedisModulesAdvancedClusterReactiveCommandsImpl<K, V> extends RedisAdvancedClusterReactiveCommandsImpl<K, V>
        implements RedisModulesAdvancedClusterReactiveCommands<K, V> {

    private final RedisModulesReactiveCommandsImpl<K, V> delegate;

    public RedisModulesAdvancedClusterReactiveCommandsImpl(StatefulRedisModulesClusterConnection<K, V> connection,
            RedisCodec<K, V> codec) {
        super(connection, codec);
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
    public Flux<Object> ftInfo(K index) {
        return delegate.ftInfo(index);
    }

    @Override
    public Flux<Value<V>> topKAdd(K key, V... items) {
        return delegate.topKAdd(key, items);
    }

    @Override
    public Flux<Value<V>> topKIncrBy(K key, LongScoredValue<V>... itemIncrements) {
        return delegate.topKIncrBy(key, itemIncrements);
    }

    @Override
    public Mono<TopKInfo> topKInfo(K key) {
        return delegate.topKInfo(key);
    }

    @Override
    public Flux<String> topKList(K key) {
        return delegate.topKList(key);
    }

    @Override
    public Flux<KeyValue<String, Long>> topKListWithScores(K key) {
        return delegate.topKListWithScores(key);
    }

    @Override
    public Flux<Boolean> topKQuery(K key, V... items) {
        return delegate.topKQuery(key, items);
    }

    @Override
    public Mono<String> topKReserve(K key, long k) {
        return delegate.topKReserve(key, k);
    }

    @Override
    public Mono<String> topKReserve(K key, long k, long width, long depth, double decay) {
        return delegate.topKReserve(key, k, width, depth, decay);
    }

    @Override
    public Mono<String> tDigestAdd(K key, double... value) {
        return delegate.tDigestAdd(key, value);
    }

    @Override
    public Flux<Double> tDigestByRank(K key, long... ranks) {
        return delegate.tDigestByRank(key, ranks);
    }

    @Override
    public Flux<Double> tDigestByRevRank(K key, long... revRanks) {
        return delegate.tDigestByRevRank(key, revRanks);
    }

    @Override
    public Flux<Double> tDigestCdf(K key, double... values) {
        return delegate.tDigestCdf(key, values);
    }

    @Override
    public Mono<String> tDigestCreate(K key) {
        return delegate.tDigestCreate(key);
    }

    @Override
    public Mono<String> tDigestCreate(K key, long compression) {
        return delegate.tDigestCreate(key, compression);
    }

    @Override
    public Mono<TDigestInfo> tDigestInfo(K key) {
        return delegate.tDigestInfo(key);
    }

    @Override
    public Mono<Double> tDigestMax(K key) {
        return delegate.tDigestMax(key);
    }

    @Override
    public Mono<String> tDigestMerge(K destinationKey, K... sourceKeys) {
        return delegate.tDigestMerge(destinationKey, sourceKeys);
    }

    @Override
    public Mono<String> tDigestMerge(K destinationKey, TDigestMergeOptions options, K... sourceKeys) {
        return delegate.tDigestMerge(destinationKey, options, sourceKeys);
    }

    @Override
    public Mono<Double> tDigestMin(K key) {
        return delegate.tDigestMin(key);
    }

    @Override
    public Flux<Double> tDigestQuantile(K key, double... quantiles) {
        return delegate.tDigestQuantile(key, quantiles);
    }

    @Override
    public Flux<Long> tDigestRank(K key, double... values) {
        return delegate.tDigestRank(key, values);
    }

    @Override
    public Mono<String> tDigestReset(K key) {
        return delegate.tDigestReset(key);
    }

    @Override
    public Flux<Long> tDigestRevRank(K key, double... values) {
        return delegate.tDigestRevRank(key, values);
    }

    @Override
    public Mono<Double> tDigestTrimmedMean(K key, double lowCutQuantile, double highCutQuantile) {
        return delegate.tDigestTrimmedMean(key, lowCutQuantile, highCutQuantile);
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
    public Flux<GetResult<K, V>> tsMget(MGetOptions<K, V> options) {
        return delegate.tsMget(options);
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
    public Flux<V> tsQueryIndex(V... filters) {
        return delegate.tsQueryIndex(filters);
    }

    @Override
    public Mono<Long> tsDel(K key, TimeRange timeRange) {
        return delegate.tsDel(key, timeRange);
    }

    @Override
    public Mono<Boolean> bfAdd(K key, V item) {
        return delegate.bfAdd(key, item);
    }

    @Override
    public Mono<Long> bfCard(K key) {
        return delegate.bfCard(key);
    }

    @Override
    public Mono<Boolean> bfExists(K key, V item) {
        return delegate.bfExists(key, item);
    }

    @Override
    public Mono<BloomFilterInfo> bfInfo(K key) {
        return delegate.bfInfo(key);
    }

    @Override
    public Mono<Long> bfInfo(K key, BloomFilterInfoType infoType) {
        return delegate.bfInfo(key, infoType);
    }

    @Override
    public Flux<Boolean> bfInsert(K key, V... items) {
        return delegate.bfInsert(key, items);
    }

    @Override
    public Flux<Boolean> bfInsert(K key, BloomFilterInsertOptions options, V... items) {
        return delegate.bfInsert(key, options, items);
    }

    @Override
    public Flux<Boolean> bfMAdd(K key, V... items) {
        return delegate.bfMAdd(key, items);
    }

    @Override
    public Flux<Boolean> bfMExists(K key, V... items) {
        return delegate.bfMExists(key, items);
    }

    @Override
    public Mono<String> bfReserve(K key, double errorRate, long capacity) {
        return delegate.bfReserve(key, errorRate, capacity);
    }

    @Override
    public Mono<String> bfReserve(K key, double errorRate, long capacity, BloomFilterReserveOptions options) {
        return delegate.bfReserve(key, errorRate, capacity, options);
    }

    @Override
    public Mono<Boolean> cfAdd(K key, V item) {
        return delegate.cfAdd(key, item);
    }

    @Override
    public Mono<Boolean> cfAddNx(K key, V item) {
        return delegate.cfAddNx(key, item);
    }

    @Override
    public Mono<Long> cfCount(K key, V item) {
        return delegate.cfCount(key, item);
    }

    @Override
    public Mono<Boolean> cfDel(K key, V item) {
        return delegate.cfDel(key, item);
    }

    @Override
    public Mono<Boolean> cfExists(K key, V item) {
        return delegate.cfExists(key, item);
    }

    @Override
    public Mono<CuckooFilter> cfInfo(K key) {
        return delegate.cfInfo(key);
    }

    @Override
    public Flux<Long> cfInsert(K key, V... items) {
        return delegate.cfInsert(key, items);
    }

    @Override
    public Flux<Long> cfInsert(K key, CuckooFilterInsertOptions options, V... items) {
        return delegate.cfInsert(key, options, items);
    }

    @Override
    public Flux<Long> cfInsertNx(K key, V... items) {
        return delegate.cfInsertNx(key, items);
    }

    @Override
    public Flux<Long> cfInsertNx(K key, CuckooFilterInsertOptions options, V... items) {
        return delegate.cfInsertNx(key, options, items);
    }

    @Override
    public Flux<Boolean> cfMExists(K key, V... items) {
        return delegate.cfMExists(key, items);
    }

    @Override
    public Mono<String> cfReserve(K key, long capacity) {
        return delegate.cfReserve(key, capacity);
    }

    @Override
    public Mono<String> cfReserve(K key, long capacity, CuckooFilterReserveOptions options) {
        return delegate.cfReserve(key, capacity, options);
    }

    @Override
    public Mono<Long> cmsIncrBy(K key, V item, long increment) {
        return delegate.cmsIncrBy(key, item, increment);
    }

    @Override
    public Flux<Long> cmsIncrBy(K key, LongScoredValue<V>... itemIncrements) {
        return delegate.cmsIncrBy(key, itemIncrements);
    }

    @Override
    public Mono<String> cmsInitByProb(K key, double error, double probability) {
        return delegate.cmsInitByProb(key, error, probability);
    }

    @Override
    public Mono<String> cmsInitByDim(K key, long width, long depth) {
        return delegate.cmsInitByDim(key, width, depth);
    }

    @Override
    public Flux<Long> cmsQuery(K key, V... items) {
        return delegate.cmsQuery(key, items);
    }

    @Override
    public Mono<String> cmsMerge(K destKey, K... keys) {
        return delegate.cmsMerge(destKey, keys);
    }

    @Override
    public Mono<String> cmsMerge(K destKey, LongScoredValue<K>... sourceKeyWeights) {
        return delegate.cmsMerge(destKey, sourceKeyWeights);
    }

    @Override
    public Mono<CmsInfo> cmsInfo(K key) {
        return delegate.cmsInfo(key);
    }

}
