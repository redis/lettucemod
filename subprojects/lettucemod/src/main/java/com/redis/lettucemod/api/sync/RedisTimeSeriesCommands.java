package com.redis.lettucemod.api.sync;

import java.util.List;

import com.redis.lettucemod.timeseries.AddOptions;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.CreateRuleOptions;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.KeySample;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;
import com.redis.lettucemod.timeseries.TimeRange;

@SuppressWarnings("unchecked")
public interface RedisTimeSeriesCommands<K, V> {

	String create(K key, CreateOptions<K, V> options);

	String alter(K key, CreateOptions<K, V> options);

	Long add(K key, long timestamp, double value);

	Long addAutoTimestamp(K key, double value);

	Long add(K key, long timestamp, double value, AddOptions<K, V> options);

	Long addAutoTimestamp(K key, double value, AddOptions<K, V> options);

	Long add(K key, Sample sample);

	Long add(K key, Sample sample, AddOptions<K, V> options);

	List<Long> madd(KeySample<K>... samples);

	Long incrby(K key, double value, Long timestamp, CreateOptions<K, V> options);

	Long decrby(K key, double value, Long timestamp, CreateOptions<K, V> options);

	Long incrbyAutoTimestamp(K key, double value, CreateOptions<K, V> options);

	Long decrbyAutoTimestamp(K key, double value, CreateOptions<K, V> options);

	String createrule(K sourceKey, K destKey, CreateRuleOptions options);

	String deleterule(K sourceKey, K destKey);

	List<Sample> range(K key, TimeRange range, RangeOptions options);

	List<Sample> revrange(K key, TimeRange range, RangeOptions options);

	List<RangeResult<K, V>> mrange(TimeRange range, MRangeOptions<K, V> options);

	List<RangeResult<K, V>> mrevrange(TimeRange range, MRangeOptions<K, V> options);

	/**
	 * Get the last sample.
	 * 
	 * @param key Key name for time series
	 * @return The last sample.
	 */
	Sample tsGet(K key);

	List<GetResult<K, V>> tsMget(V... filters);

	List<GetResult<K, V>> tsMgetWithLabels(V... filters);

	List<Object> tsInfo(K key);

	List<Object> tsInfoDebug(K key);
}
