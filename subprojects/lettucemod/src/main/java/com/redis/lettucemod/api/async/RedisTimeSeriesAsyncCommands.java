package com.redis.lettucemod.api.async;

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

import io.lettuce.core.RedisFuture;

@SuppressWarnings("unchecked")
public interface RedisTimeSeriesAsyncCommands<K, V> {

	RedisFuture<String> tsCreate(K key, CreateOptions<K, V> options);

	RedisFuture<String> tsAlter(K key, AlterOptions<K, V> options);

	RedisFuture<Long> tsAdd(K key, Sample sample);

	RedisFuture<Long> tsAdd(K key, Sample sample, AddOptions<K, V> options);

	RedisFuture<List<Long>> tsMadd(KeySample<K>... samples);

	RedisFuture<Long> tsIncrby(K key, double value);

	RedisFuture<Long> tsIncrby(K key, double value, IncrbyOptions<K, V> options);

	RedisFuture<Long> tsDecrby(K key, double value);

	RedisFuture<Long> tsDecrby(K key, double value, IncrbyOptions<K, V> options);

	RedisFuture<String> tsCreaterule(K sourceKey, K destKey, CreateRuleOptions options);

	RedisFuture<String> tsDeleterule(K sourceKey, K destKey);

	RedisFuture<List<Sample>> tsRange(K key, TimeRange range);

	RedisFuture<List<Sample>> tsRange(K key, TimeRange range, RangeOptions options);

	RedisFuture<List<Sample>> tsRevrange(K key, TimeRange range);

	RedisFuture<List<Sample>> tsRevrange(K key, TimeRange range, RangeOptions options);

	RedisFuture<List<RangeResult<K, V>>> tsMrange(TimeRange range);

	RedisFuture<List<RangeResult<K, V>>> tsMrange(TimeRange range, MRangeOptions<K, V> options);

	RedisFuture<List<RangeResult<K, V>>> tsMrevrange(TimeRange range);

	RedisFuture<List<RangeResult<K, V>>> tsMrevrange(TimeRange range, MRangeOptions<K, V> options);

	/**
	 * Get the last sample.
	 * 
	 * @param key Key name for time series
	 * @return The last sample.
	 */
	RedisFuture<Sample> tsGet(K key);

	RedisFuture<List<GetResult<K, V>>> tsMget(MGetOptions<K, V> options);

	RedisFuture<List<GetResult<K, V>>> tsMget(V... filters);

	RedisFuture<List<GetResult<K, V>>> tsMgetWithLabels(V... filters);

	RedisFuture<List<Object>> tsInfo(K key);

	RedisFuture<List<Object>> tsInfoDebug(K key);

	RedisFuture<List<V>> tsQueryIndex(V... filters);

	RedisFuture<Long> tsDel(K key, TimeRange range);
}
