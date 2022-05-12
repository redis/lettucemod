package com.redis.lettucemod.api.async;

import java.util.List;

import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.KeySample;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("unchecked")
public interface RedisTimeSeriesAsyncCommands<K, V> {

	RedisFuture<String> create(K key, CreateOptions options, Label<K, V>... labels);

	RedisFuture<String> alter(K key, CreateOptions options, Label<K, V>... labels);

	RedisFuture<Long> add(K key, long timestamp, double value);

	RedisFuture<Long> addAutoTimestamp(K key, double value);

	RedisFuture<Long> add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels);

	RedisFuture<Long> addAutoTimestamp(K key, double value, CreateOptions options, Label<K, V>... labels);

	RedisFuture<Long> add(K key, Sample sample);

	RedisFuture<Long> add(K key, Sample sample, CreateOptions options, Label<K, V>... labels);

	RedisFuture<List<Long>> madd(KeySample<K>... samples);

	RedisFuture<Long> incrby(K key, double value, Long timestamp, CreateOptions options, Label<K, V>... labels);

	RedisFuture<Long> decrby(K key, double value, Long timestamp, CreateOptions options, Label<K, V>... labels);

	RedisFuture<Long> incrbyAutoTimestamp(K key, double value, CreateOptions options, Label<K, V>... labels);

	RedisFuture<Long> decrbyAutoTimestamp(K key, double value, CreateOptions options, Label<K, V>... labels);

	RedisFuture<String> createrule(K sourceKey, K destKey, Aggregation aggregation);

	RedisFuture<String> deleterule(K sourceKey, K destKey);

	RedisFuture<List<Sample>> range(K key, RangeOptions options);

	RedisFuture<List<Sample>> revrange(K key, RangeOptions options);

	RedisFuture<List<RangeResult<K, V>>> mrange(RangeOptions options, V... filters);

	RedisFuture<List<RangeResult<K, V>>> mrevrange(RangeOptions options, V... filters);

	RedisFuture<List<RangeResult<K, V>>> mrangeWithLabels(RangeOptions options, V... filters);

	RedisFuture<List<RangeResult<K, V>>> mrevrangeWithLabels(RangeOptions options, V... filters);

	RedisFuture<Sample> tsGet(K key);

	RedisFuture<List<GetResult<K, V>>> tsMget(V... filters);

	RedisFuture<List<GetResult<K, V>>> tsMgetWithLabels(V... filters);

	RedisFuture<List<Object>> tsInfo(K key);

	RedisFuture<List<Object>> tsInfoDebug(K key);

}
