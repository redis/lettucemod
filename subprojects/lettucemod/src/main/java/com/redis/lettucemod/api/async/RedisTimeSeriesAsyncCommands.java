package com.redis.lettucemod.api.async;

import java.util.List;

import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.KeySample;
import com.redis.lettucemod.timeseries.MRangeOptions;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("unchecked")
public interface RedisTimeSeriesAsyncCommands<K, V> {

	RedisFuture<String> create(K key, CreateOptions<K, V> options);

	RedisFuture<String> alter(K key, CreateOptions<K, V> options);

	RedisFuture<Long> add(K key, long timestamp, double value);

	RedisFuture<Long> addAutoTimestamp(K key, double value);

	RedisFuture<Long> add(K key, long timestamp, double value, CreateOptions<K, V> options);

	RedisFuture<Long> addAutoTimestamp(K key, double value, CreateOptions<K, V> options);

	RedisFuture<Long> add(K key, Sample sample);

	RedisFuture<Long> add(K key, Sample sample, CreateOptions<K, V> options);

	RedisFuture<List<Long>> madd(KeySample<K>... samples);

	RedisFuture<Long> incrby(K key, double value, Long timestamp, CreateOptions<K, V> options);

	RedisFuture<Long> decrby(K key, double value, Long timestamp, CreateOptions<K, V> options);

	RedisFuture<Long> incrbyAutoTimestamp(K key, double value, CreateOptions<K, V> options);

	RedisFuture<Long> decrbyAutoTimestamp(K key, double value, CreateOptions<K, V> options);

	RedisFuture<String> createrule(K sourceKey, K destKey, Aggregation aggregation);

	RedisFuture<String> deleterule(K sourceKey, K destKey);

	RedisFuture<List<Sample>> range(K key, RangeOptions options);

	RedisFuture<List<Sample>> revrange(K key, RangeOptions options);

	RedisFuture<List<RangeResult<K, V>>> mrange(MRangeOptions<K, V> options);

	RedisFuture<List<RangeResult<K, V>>> mrevrange(MRangeOptions<K, V> options);

	RedisFuture<Sample> tsGet(K key);

	RedisFuture<List<GetResult<K, V>>> tsMget(V... filters);

	RedisFuture<List<GetResult<K, V>>> tsMgetWithLabels(V... filters);

	RedisFuture<List<Object>> tsInfo(K key);

	RedisFuture<List<Object>> tsInfoDebug(K key);

}
