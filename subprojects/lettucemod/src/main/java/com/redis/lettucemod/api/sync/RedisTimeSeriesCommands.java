package com.redis.lettucemod.api.sync;

import java.util.List;

import com.redis.lettucemod.timeseries.Aggregation;
import com.redis.lettucemod.timeseries.CreateOptions;
import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.KeySample;
import com.redis.lettucemod.timeseries.Label;
import com.redis.lettucemod.timeseries.RangeOptions;
import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;

@SuppressWarnings("unchecked")
public interface RedisTimeSeriesCommands<K, V> {

	String create(K key, CreateOptions options, Label<K, V>... labels);

	String alter(K key, CreateOptions options, Label<K, V>... labels);

	Long add(K key, long timestamp, double value);

	Long addAutoTimestamp(K key, double value);

	Long add(K key, long timestamp, double value, CreateOptions options, Label<K, V>... labels);

	Long addAutoTimestamp(K key, double value, CreateOptions options, Label<K, V>... labels);

	Long add(K key, Sample sample);

	Long add(K key, Sample sample, CreateOptions options, Label<K, V>... labels);

	List<Long> madd(KeySample<K>... samples);

	Long incrby(K key, double value, Long timestamp, CreateOptions options, Label<K, V>... labels);

	Long decrby(K key, double value, Long timestamp, CreateOptions options, Label<K, V>... labels);

	Long incrbyAutoTimestamp(K key, double value, CreateOptions options, Label<K, V>... labels);

	Long decrbyAutoTimestamp(K key, double value, CreateOptions options, Label<K, V>... labels);

	String createrule(K sourceKey, K destKey, Aggregation aggregation);

	String deleterule(K sourceKey, K destKey);

	List<Sample> range(K key, RangeOptions options);

	List<Sample> revrange(K key, RangeOptions options);

	List<RangeResult<K, V>> mrange(RangeOptions options, V... filters);

	List<RangeResult<K, V>> mrevrange(RangeOptions options, V... filters);

	List<RangeResult<K, V>> mrangeWithLabels(RangeOptions options, V... filters);

	List<RangeResult<K, V>> mrevrangeWithLabels(RangeOptions options, V... filters);

	Sample tsGet(K key);

	List<GetResult<K, V>> tsMget(V... filters);

	List<GetResult<K, V>> tsMgetWithLabels(V... filters);

	List<Object> tsInfo(K key);

	List<Object> tsInfoDebug(K key);
}
