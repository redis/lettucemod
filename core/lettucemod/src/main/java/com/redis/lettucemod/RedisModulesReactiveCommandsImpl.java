package com.redis.lettucemod;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.reactive.RedisModulesReactiveCommands;
import com.redis.lettucemod.bloom.*;
import com.redis.lettucemod.search.SearchCommandBuilder;
import com.redis.lettucemod.timeseries.*;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisReactiveCommandsImpl;
import io.lettuce.core.Value;
import io.lettuce.core.codec.RedisCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
public class RedisModulesReactiveCommandsImpl<K, V> extends RedisReactiveCommandsImpl<K, V>
        implements RedisModulesReactiveCommands<K, V> {

    private final StatefulRedisModulesConnection<K, V> connection;

    private final TimeSeriesCommandBuilder<K, V> timeSeriesCommandBuilder;

    private final SearchCommandBuilder<K, V> searchCommandBuilder;

    private final BloomCommandBuilder<K, V> bloomCommandBuilder;

    public RedisModulesReactiveCommandsImpl(StatefulRedisModulesConnection<K, V> connection, RedisCodec<K, V> codec) {
        super(connection, codec);
        this.connection = connection;
        this.timeSeriesCommandBuilder = new TimeSeriesCommandBuilder<>(codec);
        this.searchCommandBuilder = new SearchCommandBuilder<>(codec);
        this.bloomCommandBuilder = new BloomCommandBuilder<>(codec);
    }

    @Override
    public StatefulRedisModulesConnection<K, V> getStatefulConnection() {
        return connection;
    }

    @Override
    public Mono<String> tsCreate(K key, CreateOptions<K, V> options) {
        return createMono(() -> timeSeriesCommandBuilder.create(key, options));
    }

    @Override
    public Mono<String> tsAlter(K key, AlterOptions<K, V> options) {
        return createMono(() -> timeSeriesCommandBuilder.alter(key, options));
    }

    @Override
    public Mono<Long> tsAdd(K key, Sample sample) {
        return createMono(() -> timeSeriesCommandBuilder.add(key, sample));
    }

    @Override
    public Mono<Long> tsAdd(K key, Sample sample, AddOptions<K, V> options) {
        return createMono(() -> timeSeriesCommandBuilder.add(key, sample, options));
    }

    @Override
    public Mono<Long> tsIncrby(K key, double value) {
        return createMono(() -> timeSeriesCommandBuilder.incrby(key, value, null));
    }

    @Override
    public Mono<Long> tsIncrby(K key, double value, IncrbyOptions<K, V> options) {
        return createMono(() -> timeSeriesCommandBuilder.incrby(key, value, options));
    }

    @Override
    public Mono<Long> tsDecrby(K key, double value) {
        return createMono(() -> timeSeriesCommandBuilder.decrby(key, value, null));
    }

    @Override
    public Mono<Long> tsDecrby(K key, double value, IncrbyOptions<K, V> options) {
        return createMono(() -> timeSeriesCommandBuilder.decrby(key, value, options));
    }

    @Override
    public Flux<Long> tsMadd(KeySample<K>... samples) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.madd(samples));
    }

    @Override
    public Mono<String> tsCreaterule(K sourceKey, K destKey, CreateRuleOptions options) {
        return createMono(() -> timeSeriesCommandBuilder.createRule(sourceKey, destKey, options));
    }

    @Override
    public Mono<String> tsDeleterule(K sourceKey, K destKey) {
        return createMono(() -> timeSeriesCommandBuilder.deleteRule(sourceKey, destKey));
    }

    @Override
    public Flux<Sample> tsRange(K key, TimeRange range) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.range(key, range));
    }

    @Override
    public Flux<Sample> tsRange(K key, TimeRange range, RangeOptions options) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.range(key, range, options));
    }

    @Override
    public Flux<Sample> tsRevrange(K key, TimeRange range) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.revrange(key, range));
    }

    @Override
    public Flux<Sample> tsRevrange(K key, TimeRange range, RangeOptions options) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.revrange(key, range, options));
    }

    @Override
    public Flux<RangeResult<K, V>> tsMrange(TimeRange range) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.mrange(range));
    }

    @Override
    public Flux<RangeResult<K, V>> tsMrange(TimeRange range, MRangeOptions<K, V> options) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.mrange(range, options));
    }

    @Override
    public Flux<RangeResult<K, V>> tsMrevrange(TimeRange range) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.mrevrange(range));
    }

    @Override
    public Flux<RangeResult<K, V>> tsMrevrange(TimeRange range, MRangeOptions<K, V> options) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.mrevrange(range, options));
    }

    @Override
    public Mono<Sample> tsGet(K key) {
        return createMono(() -> timeSeriesCommandBuilder.get(key));
    }

    @Override
    public Flux<GetResult<K, V>> tsMget(MGetOptions<K, V> options) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.mget(options));
    }

    @Override
    public Flux<GetResult<K, V>> tsMget(V... filters) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.mget(filters));
    }

    @Override
    public Flux<GetResult<K, V>> tsMgetWithLabels(V... filters) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.mgetWithLabels(filters));
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
    public Flux<V> tsQueryIndex(V... filters) {
        return createDissolvingFlux(() -> timeSeriesCommandBuilder.queryIndex(filters));
    }

    @Override
    public Mono<Long> tsDel(K key, TimeRange timeRange) {
        return createMono(() -> timeSeriesCommandBuilder.tsDel(key, timeRange));
    }

    @Override
    public Flux<Object> ftInfo(K index) {
        return createDissolvingFlux(() -> searchCommandBuilder.info(index));
    }

    @Override
    public Flux<Value<V>> topKAdd(K key, V... items) {
        return createDissolvingFlux(() -> bloomCommandBuilder.topKAdd(key, items));
    }

    @Override
    public Flux<Value<V>> topKIncrBy(K key, LongScoredValue<V>... itemIncrements) {
        return createDissolvingFlux(() -> bloomCommandBuilder.topKIncrBy(key, itemIncrements));
    }

    @Override
    public Mono<TopKInfo> topKInfo(K key) {
        return createMono(() -> bloomCommandBuilder.topKInfo(key));
    }

    @Override
    public Flux<String> topKList(K key) {
        return createDissolvingFlux(() -> bloomCommandBuilder.topKList(key));
    }

    @Override
    public Flux<KeyValue<String, Long>> topKListWithScores(K key) {
        return createDissolvingFlux(() -> bloomCommandBuilder.topKListWithScores(key));
    }

    @Override
    public Flux<Boolean> topKQuery(K key, V... items) {
        return createDissolvingFlux(() -> bloomCommandBuilder.topKQuery(key, items));
    }

    @Override
    public Mono<String> topKReserve(K key, long k) {
        return createMono(() -> bloomCommandBuilder.topKReserve(key, k));
    }

    @Override
    public Mono<String> topKReserve(K key, long k, long width, long depth, double decay) {
        return createMono(() -> bloomCommandBuilder.topKReserve(key, k, width, depth, decay));
    }

    @Override
    public Mono<String> tDigestAdd(K key, double... values) {
        return createMono(() -> bloomCommandBuilder.tDigestAdd(key, values));
    }

    @Override
    public Flux<Double> tDigestByRank(K key, long... ranks) {
        return createDissolvingFlux(() -> bloomCommandBuilder.tDigestByRank(key, ranks));
    }

    @Override
    public Flux<Double> tDigestByRevRank(K key, long... revRanks) {
        return createDissolvingFlux(() -> bloomCommandBuilder.tDigestByRevRank(key, revRanks));
    }

    @Override
    public Flux<Double> tDigestCdf(K key, double... values) {
        return createDissolvingFlux(() -> bloomCommandBuilder.tDigestCdf(key, values));
    }

    @Override
    public Mono<String> tDigestCreate(K key) {
        return createMono(() -> bloomCommandBuilder.tDigestCreate(key));
    }

    @Override
    public Mono<String> tDigestCreate(K key, long compression) {
        return createMono(() -> bloomCommandBuilder.tDigestCreate(key, compression));
    }

    @Override
    public Mono<TDigestInfo> tDigestInfo(K key) {
        return createMono(() -> bloomCommandBuilder.tDigestInfo(key));
    }

    @Override
    public Mono<Double> tDigestMax(K key) {
        return createMono(() -> bloomCommandBuilder.tDigestMax(key));
    }

    @Override
    public Mono<String> tDigestMerge(K destinationKey, K... sourceKeys) {
        return createMono(() -> bloomCommandBuilder.tDigestMerge(destinationKey, sourceKeys));
    }

    @Override
    public Mono<String> tDigestMerge(K destinationKey, TDigestMergeOptions options, K... sourceKeys) {
        return createMono(() -> bloomCommandBuilder.tDigestMerge(destinationKey, options, sourceKeys));
    }

    @Override
    public Mono<Double> tDigestMin(K key) {
        return createMono(() -> bloomCommandBuilder.tDigestMin(key));
    }

    @Override
    public Flux<Double> tDigestQuantile(K key, double... quantiles) {
        return createDissolvingFlux(() -> bloomCommandBuilder.tDigestQuantile(key, quantiles));
    }

    @Override
    public Flux<Long> tDigestRank(K key, double... values) {
        return createDissolvingFlux(() -> bloomCommandBuilder.tDigestRank(key, values));
    }

    @Override
    public Mono<String> tDigestReset(K key) {
        return createMono(() -> bloomCommandBuilder.tDigestReset(key));
    }

    @Override
    public Flux<Long> tDigestRevRank(K key, double... values) {
        return createDissolvingFlux(() -> bloomCommandBuilder.tDigestRevRank(key, values));
    }

    @Override
    public Mono<Double> tDigestTrimmedMean(K key, double lowCutQuantile, double highCutQuantile) {
        return createMono(() -> bloomCommandBuilder.tDigestTrimmedMean(key, lowCutQuantile, highCutQuantile));
    }

    @Override
    public Mono<Boolean> bfAdd(K key, V item) {
        return createMono(() -> bloomCommandBuilder.bfAdd(key, item));
    }

    @Override
    public Mono<Long> bfCard(K key) {
        return createMono(() -> bloomCommandBuilder.bfCard(key));
    }

    @Override
    public Mono<Boolean> bfExists(K key, V item) {
        return createMono(() -> bloomCommandBuilder.bfExists(key, item));
    }

    @Override
    public Mono<BloomFilterInfo> bfInfo(K key) {
        return createMono(() -> bloomCommandBuilder.bfInfo(key));
    }

    @Override
    public Mono<Long> bfInfo(K key, BloomFilterInfoType infoType) {
        return createMono(() -> bloomCommandBuilder.bfInfo(key, infoType));
    }

    @Override
    public Flux<Boolean> bfInsert(K key, V... items) {
        return createDissolvingFlux(() -> bloomCommandBuilder.bfInsert(key, items));
    }

    @Override
    public Flux<Boolean> bfInsert(K key, BloomFilterInsertOptions options, V... items) {
        return createDissolvingFlux(() -> bloomCommandBuilder.bfInsert(key, options, items));
    }

    @Override
    public Flux<Boolean> bfMAdd(K key, V... items) {
        return createDissolvingFlux(() -> bloomCommandBuilder.bfMAdd(key, items));
    }

    @Override
    public Flux<Boolean> bfMExists(K key, V... items) {
        return createDissolvingFlux(() -> bloomCommandBuilder.bfMExists(key, items));
    }

    @Override
    public Mono<String> bfReserve(K key, double errorRate, long capacity) {
        return bfReserve(key, errorRate, capacity, null);
    }

    @Override
    public Mono<String> bfReserve(K key, double errorRate, long capacity, BloomFilterReserveOptions options) {
        return createMono(() -> bloomCommandBuilder.bfReserve(key, errorRate, capacity, options));
    }

    @Override
    public Mono<Boolean> cfAdd(K key, V item) {
        return createMono(() -> bloomCommandBuilder.cfAdd(key, item));
    }

    @Override
    public Mono<Boolean> cfAddNx(K key, V item) {
        return createMono(() -> bloomCommandBuilder.cfAddNx(key, item));
    }

    @Override
    public Mono<Long> cfCount(K key, V item) {
        return createMono(() -> bloomCommandBuilder.cfCount(key, item));
    }

    @Override
    public Mono<Boolean> cfDel(K key, V item) {
        return createMono(() -> bloomCommandBuilder.cfDel(key, item));
    }

    @Override
    public Mono<Boolean> cfExists(K key, V item) {
        return createMono(() -> bloomCommandBuilder.cfExists(key, item));
    }

    @Override
    public Mono<CuckooFilter> cfInfo(K key) {
        return createMono(() -> bloomCommandBuilder.cfInfo(key));
    }

    @Override
    public Flux<Long> cfInsert(K key, V... items) {
        return createDissolvingFlux(() -> bloomCommandBuilder.cfInsert(key, items));
    }

    @Override
    public Flux<Long> cfInsert(K key, CuckooFilterInsertOptions options, V... items) {
        return createDissolvingFlux(() -> bloomCommandBuilder.cfInsert(key, items, options));
    }

    @Override
    public Flux<Long> cfInsertNx(K key, V... items) {
        return createDissolvingFlux(() -> bloomCommandBuilder.cfInsertNx(key, items));
    }

    @Override
    public Flux<Long> cfInsertNx(K key, CuckooFilterInsertOptions options, V... items) {
        return createDissolvingFlux(() -> bloomCommandBuilder.cfInsertNx(key, items, options));
    }

    @Override
    public Flux<Boolean> cfMExists(K key, V... items) {
        return createDissolvingFlux(() -> bloomCommandBuilder.cfMExists(key, items));
    }

    @Override
    public Mono<String> cfReserve(K key, long capacity) {
        return cfReserve(key, capacity, null);
    }

    @Override
    public Mono<String> cfReserve(K key, long capacity, CuckooFilterReserveOptions options) {
        return createMono(() -> bloomCommandBuilder.cfReserve(key, capacity, options));
    }

    @Override
    public Mono<Long> cmsIncrBy(K key, V item, long increment) {
        return createMono(() -> bloomCommandBuilder.cmsIncrBy(key, item, increment));
    }

    @Override
    public Flux<Long> cmsIncrBy(K key, LongScoredValue<V>... itemIncrements) {
        return createDissolvingFlux(() -> bloomCommandBuilder.cmsIncrBy(key, itemIncrements));
    }

    @Override
    public Mono<String> cmsInitByProb(K key, double error, double probability) {
        return createMono(() -> bloomCommandBuilder.cmsInitByProb(key, error, probability));
    }

    @Override
    public Mono<String> cmsInitByDim(K key, long width, long depth) {
        return createMono(() -> bloomCommandBuilder.cmsInitByDim(key, width, depth));
    }

    @Override
    public Flux<Long> cmsQuery(K key, V... items) {
        return createDissolvingFlux(() -> bloomCommandBuilder.cmsQuery(key, items));
    }

    @Override
    public Mono<String> cmsMerge(K destKey, K... keys) {
        return createMono(() -> bloomCommandBuilder.cmsMerge(destKey, keys));
    }

    @Override
    public Mono<String> cmsMerge(K destKey, LongScoredValue<K>... sourceKeyWeights) {
        return createMono(() -> bloomCommandBuilder.cmsMerge(destKey, sourceKeyWeights));
    }

    @Override
    public Mono<CmsInfo> cmsInfo(K key) {
        return createMono(() -> bloomCommandBuilder.cmsInfo(key));
    }

}
