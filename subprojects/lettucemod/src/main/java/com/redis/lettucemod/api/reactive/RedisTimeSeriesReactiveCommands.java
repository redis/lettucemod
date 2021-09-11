package com.redis.lettucemod.api.reactive;

import com.redis.lettucemod.api.timeseries.Aggregation;
import com.redis.lettucemod.api.timeseries.CreateOptions;
import com.redis.lettucemod.api.timeseries.GetResult;
import com.redis.lettucemod.api.timeseries.KeySample;
import com.redis.lettucemod.api.timeseries.RangeOptions;
import com.redis.lettucemod.api.timeseries.RangeResult;
import com.redis.lettucemod.api.timeseries.Sample;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RedisTimeSeriesReactiveCommands<K, V> {

    Mono<String> create(K key, CreateOptions<K, V> options);

    Mono<String> alter(K key, CreateOptions<K, V> options);

    Mono<Long> add(K key, long timestamp, double value);

    Mono<Long> addAutoTimestamp(K key, double value);

    Mono<Long> add(K key, long timestamp, double value, CreateOptions<K, V> options);

    Mono<Long> addAutoTimestamp(K key, double value, CreateOptions<K, V> options);

    Mono<Long> add(K key, Sample sample);

    Mono<Long> add(K key, Sample sample, CreateOptions<K, V> options);

    Flux<Long> madd(KeySample<K>... samples);

    Mono<Long> incrby(K key, double value, Long timestamp, CreateOptions<K, V> options);

    Mono<Long> decrby(K key, double value, Long timestamp, CreateOptions<K, V> options);

    Mono<Long> incrbyAutoTimestamp(K key, double value, CreateOptions<K, V> options);

    Mono<Long> decrbyAutoTimestamp(K key, double value, CreateOptions<K, V> options);

    Mono<String> createrule(K sourceKey, K destKey, Aggregation aggregation);

    Mono<String> deleterule(K sourceKey, K destKey);

    Flux<Sample> range(K key, RangeOptions options);

    Flux<Sample> revrange(K key, RangeOptions options);

    Flux<RangeResult<K, V>> mrange(RangeOptions options, V... filters);

    Flux<RangeResult<K, V>> mrevrange(RangeOptions options, V... filters);

    Flux<RangeResult<K, V>> mrangeWithLabels(RangeOptions options, V... filters);

    Flux<RangeResult<K, V>> mrevrangeWithLabels(RangeOptions options, V... filters);

    Mono<Sample> tsGet(K key);

    Flux<GetResult<K, V>> tsMget(V... filters);

    Flux<GetResult<K, V>> tsMgetWithLabels(V... filters);

    Flux<Object> tsInfo(K key);

    Flux<Object> tsInfoDebug(K key);


}
