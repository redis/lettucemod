package com.redis.lettucemod.api.async;

import java.util.List;
import java.util.Map;

import com.redis.lettucemod.bloom.BloomFilterInfo;
import com.redis.lettucemod.bloom.BloomFilterInfoType;
import com.redis.lettucemod.bloom.BloomFilterInsertOptions;
import com.redis.lettucemod.bloom.BloomFilterReserveOptions;
import com.redis.lettucemod.bloom.CuckooFilter;
import com.redis.lettucemod.bloom.CuckooFilterInsertOptions;
import com.redis.lettucemod.bloom.CuckooFilterReserveOptions;
import com.redis.lettucemod.bloom.LongScoredValue;
import com.redis.lettucemod.bloom.TDigestInfo;
import com.redis.lettucemod.bloom.TDigestMergeOptions;
import com.redis.lettucemod.bloom.TopKInfo;
import com.redis.lettucemod.cms.CmsInfo;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.Value;

@SuppressWarnings("unchecked")
public interface RedisBloomAsyncCommands<K, V> {

	RedisFuture<Boolean> bfAdd(K key, V item);

	RedisFuture<Long> bfCard(K key);

	RedisFuture<Boolean> bfExists(K key, V item);

	RedisFuture<BloomFilterInfo> bfInfo(K key);

	RedisFuture<Long> bfInfo(K key, BloomFilterInfoType infoType);

	RedisFuture<List<Boolean>> bfInsert(K key, V... items);

	RedisFuture<List<Boolean>> bfInsert(K key, BloomFilterInsertOptions options, V... items);

	RedisFuture<List<Boolean>> bfMAdd(K key, V... items);

	RedisFuture<List<Boolean>> bfMExists(K key, V... items);

	RedisFuture<String> bfReserve(K key, double errorRate, long capacity);

	RedisFuture<String> bfReserve(K key, double errorRate, long capacity, BloomFilterReserveOptions options);

	RedisFuture<Boolean> cfAdd(K key, V item);

	RedisFuture<Boolean> cfAddNx(K key, V item);

	RedisFuture<Long> cfCount(K key, V item);

	RedisFuture<Boolean> cfDel(K key, V item);

	RedisFuture<Boolean> cfExists(K key, V item);

	RedisFuture<CuckooFilter> cfInfo(K key);

	RedisFuture<List<Long>> cfInsert(K key, V... items);

	RedisFuture<List<Long>> cfInsert(K key, CuckooFilterInsertOptions options, V... items);

	RedisFuture<List<Long>> cfInsertNx(K key, V... items);

	RedisFuture<List<Long>> cfInsertNx(K key, CuckooFilterInsertOptions options, V... items);

	RedisFuture<List<Boolean>> cfMExists(K key, V... items);

	RedisFuture<String> cfReserve(K key, long capacity);

	RedisFuture<String> cfReserve(K key, long capacity, CuckooFilterReserveOptions options);

	RedisFuture<Long> cmsIncrBy(K key, V item, long increment);

	RedisFuture<List<Long>> cmsIncrBy(K key, LongScoredValue<V>... itemIncrements);

	RedisFuture<String> cmsInitByProb(K key, double error, double probability);

	RedisFuture<String> cmsInitByDim(K key, long width, long depth);

	RedisFuture<List<Long>> cmsQuery(K key, V... items);

	RedisFuture<String> cmsMerge(K destKey, K... keys);

	RedisFuture<String> cmsMerge(K destKey, LongScoredValue<K>... sourceKeyWeights);

	RedisFuture<CmsInfo> cmsInfo(K key);

	RedisFuture<List<Value<V>>> topKAdd(K key, V... items);

	RedisFuture<List<Value<V>>> topKIncrBy(K key, LongScoredValue<V>... itemIncrements);

	RedisFuture<TopKInfo> topKInfo(K key);

	RedisFuture<List<String>> topKList(K key);

	RedisFuture<List<KeyValue<String, Long>>> topKListWithScores(K key);

	RedisFuture<List<Boolean>> topKQuery(K key, V... items);

	RedisFuture<String> topKReserve(K key, long k);

	RedisFuture<String> topKReserve(K key, long k, long width, long depth, double decay);

	RedisFuture<String> tDigestAdd(K key, double... value);

	RedisFuture<List<Double>> tDigestByRank(K key, long... ranks);

	RedisFuture<List<Double>> tDigestByRevRank(K key, long... revRanks);

	RedisFuture<List<Double>> tDigestCdf(K key, double... values);

	RedisFuture<String> tDigestCreate(K key);

	RedisFuture<String> tDigestCreate(K key, long compression);

	RedisFuture<TDigestInfo> tDigestInfo(K key);

	RedisFuture<Double> tDigestMax(K key);

	RedisFuture<String> tDigestMerge(K destinationKey, K... sourceKeys);

	RedisFuture<String> tDigestMerge(K destinationKey, TDigestMergeOptions options, K... sourceKeys);

	RedisFuture<Double> tDigestMin(K key);

	RedisFuture<List<Double>> tDigestQuantile(K key, double... quantiles);

	RedisFuture<List<Long>> tDigestRank(K key, double... values);

	RedisFuture<String> tDigestReset(K key);

	RedisFuture<List<Long>> tDigestRevRank(K key, double... values);

	RedisFuture<Double> tDigestTrimmedMean(K key, double lowCutQuantile, double highCutQuantile);
}
