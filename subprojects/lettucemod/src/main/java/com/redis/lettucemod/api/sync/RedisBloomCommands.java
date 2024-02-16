package com.redis.lettucemod.api.sync;

import java.util.List;

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
import io.lettuce.core.Value;

@SuppressWarnings("unchecked")
public interface RedisBloomCommands<K, V> {

	Boolean bfAdd(K key, V item);

	Long bfCard(K key);

	Boolean bfExists(K key, V item);

	BloomFilterInfo bfInfo(K key);

	Long bfInfo(K key, BloomFilterInfoType type);

	List<Boolean> bfInsert(K key, V... items);

	List<Boolean> bfInsert(K key, BloomFilterInsertOptions options, V... items);

	List<Boolean> bfMAdd(K key, V... items);

	List<Boolean> bfMExists(K key, V... items);

	String bfReserve(K key, double errorRate, long capacity);

	String bfReserve(K key, double errorRate, long capacity, BloomFilterReserveOptions options);

	Boolean cfAdd(K key, V item);

	Boolean cfAddNx(K key, V item);

	Long cfCount(K key, V item);

	Boolean cfDel(K key, V item);

	Boolean cfExists(K key, V item);

	CuckooFilter cfInfo(K key);

	List<Long> cfInsert(K key, V... items);

	List<Long> cfInsert(K key, CuckooFilterInsertOptions options, V... items);

	List<Long> cfInsertNx(K key, V... items);

	List<Long> cfInsertNx(K key, CuckooFilterInsertOptions options, V... items);

	List<Boolean> cfMExists(K key, V... items);

	String cfReserve(K key, long capacity);

	String cfRserve(K key, long capacity, CuckooFilterReserveOptions options);

	Long cmsIncrBy(K key, V item, long increment);

	List<Long> cmsIncrBy(K key, LongScoredValue<V>... itemIncrements);

	String cmsInitByProb(K key, double error, double probability);

	String cmsInitByDim(K key, long width, long depth);

	List<Long> cmsQuery(K key, V... items);

	String cmsMerge(K destKey, K... keys);

	String cmsMerge(K destKey, LongScoredValue<K>... sourceKeyWeights);

	CmsInfo cmsInfo(K key);

	List<Value<V>> topKAdd(K key, V... items);

	List<Value<V>> topKIncrBy(K key, LongScoredValue<V>... itemIncrements);

	TopKInfo topKInfo(K key);

	List<String> topKList(K key);

	List<KeyValue<String, Long>> topKListWithScores(K key);

	List<Boolean> topKQuery(K key, V... items);

	String topKReserve(K key, long k);

	String topKReserve(K key, long k, long width, long depth, double decay);

	String tDigestAdd(K key, double... values);

	List<Double> tDigestByRank(K key, long... ranks);

	List<Double> tDigestByRevRank(K key, long... revRanks);

	List<Double> tDigestCdf(K key, double... values);

	String tDigestCreate(K key);

	String tDigestCreate(K key, long compression);

	TDigestInfo tDigestInfo(K key);

	Double tDigestMax(K key);

	String tDigestMerge(K destinationKey, K... sourceKeys);

	String tDigestMerge(K destinationKey, TDigestMergeOptions options, K... sourceKeys);

	Double tDigestMin(K key);

	List<Double> tDigestQuantile(K key, double... quantiles);

	List<Long> tDigestRank(K key, double... values);

	String tDigestReset(K key);

	List<Long> tDigestRevRank(K key, double... values);

	Double tDigestTrimmedMean(K key, double lowCutQuantile, double highCutQuantile);
}
