package com.redis.lettucemod.api.sync;

import com.redis.lettucemod.api.timeseries.Aggregation;
import com.redis.lettucemod.api.timeseries.CreateOptions;
import com.redis.lettucemod.api.timeseries.GetResult;
import com.redis.lettucemod.api.timeseries.KeySample;
import com.redis.lettucemod.api.timeseries.RangeOptions;
import com.redis.lettucemod.api.timeseries.RangeResult;
import com.redis.lettucemod.api.timeseries.Sample;

import java.util.List;

public interface RedisTimeSeriesCommands<K, V> {

    String create(K key, CreateOptions<K, V> options);

    String alter(K key, CreateOptions<K, V> options);

    Long add(K key, long timestamp, double value);

    Long add(K key, long timestamp, double value, CreateOptions<K, V> options);

    List<Long> madd(KeySample<K>... samples);

    Long incrby(K key, double value, Long timestamp, CreateOptions<K, V> options);

    Long decrby(K key, double value, Long timestamp, CreateOptions<K, V> options);

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
