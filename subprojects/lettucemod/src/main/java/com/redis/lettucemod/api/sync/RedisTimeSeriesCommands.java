package com.redis.lettucemod.api.sync;

import java.util.List;

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

@SuppressWarnings("unchecked")
public interface RedisTimeSeriesCommands<K, V> {

	String tsCreate(K key, CreateOptions<K, V> options);

	String tsAlter(K key, AlterOptions<K, V> options);

	Long tsAdd(K key, Sample sample);

	Long tsAdd(K key, Sample sample, AddOptions<K, V> options);

	List<Long> tsMadd(KeySample<K>... samples);

	Long tsIncrby(K key, double value);

	Long tsIncrby(K key, double value, IncrbyOptions<K, V> options);

	Long tsDecrby(K key, double value);

	Long tsDecrby(K key, double value, IncrbyOptions<K, V> options);

	String tsCreaterule(K sourceKey, K destKey, CreateRuleOptions options);

	String tsDeleterule(K sourceKey, K destKey);

	List<Sample> tsRange(K key, TimeRange range);

	List<Sample> tsRange(K key, TimeRange range, RangeOptions options);

	List<Sample> tsRevrange(K key, TimeRange range);

	List<Sample> tsRevrange(K key, TimeRange range, RangeOptions options);

	List<RangeResult<K, V>> tsMrange(TimeRange range);

	List<RangeResult<K, V>> tsMrange(TimeRange range, MRangeOptions<K, V> options);

	List<RangeResult<K, V>> tsMrevrange(TimeRange range);

	List<RangeResult<K, V>> tsMrevrange(TimeRange range, MRangeOptions<K, V> options);

	/**
	 * Get the last sample.
	 * 
	 * @param key Key name for time series
	 * @return The last sample.
	 */
	Sample tsGet(K key);

	List<GetResult<K, V>> tsMget(MGetOptions<K, V> options);
	
	List<GetResult<K, V>> tsMget(V... filters);

	List<GetResult<K, V>> tsMgetWithLabels(V... filters);

	List<Object> tsInfo(K key);

	List<Object> tsInfoDebug(K key);
}
