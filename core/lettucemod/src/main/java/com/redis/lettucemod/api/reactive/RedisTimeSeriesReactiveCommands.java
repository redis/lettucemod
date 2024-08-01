package com.redis.lettucemod.api.reactive;

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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("unchecked")
public interface RedisTimeSeriesReactiveCommands<K, V> {

	Mono<String> tsCreate(K key, CreateOptions<K, V> options);

	Mono<String> tsAlter(K key, AlterOptions<K, V> options);

	Mono<Long> tsAdd(K key, Sample sample);

	Mono<Long> tsAdd(K key, Sample sample, AddOptions<K, V> options);

	Flux<Long> tsMadd(KeySample<K>... samples);

	Mono<Long> tsIncrby(K key, double value);

	Mono<Long> tsIncrby(K key, double value, IncrbyOptions<K, V> options);

	Mono<Long> tsDecrby(K key, double value);

	Mono<Long> tsDecrby(K key, double value, IncrbyOptions<K, V> options);

	Mono<String> tsCreaterule(K sourceKey, K destKey, CreateRuleOptions options);

	Mono<String> tsDeleterule(K sourceKey, K destKey);

	Flux<Sample> tsRange(K key, TimeRange range);

	Flux<Sample> tsRange(K key, TimeRange range, RangeOptions options);

	Flux<Sample> tsRevrange(K key, TimeRange range);

	Flux<Sample> tsRevrange(K key, TimeRange range, RangeOptions options);

	Flux<RangeResult<K, V>> tsMrange(TimeRange range);

	Flux<RangeResult<K, V>> tsMrange(TimeRange range, MRangeOptions<K, V> options);

	Flux<RangeResult<K, V>> tsMrevrange(TimeRange range);

	Flux<RangeResult<K, V>> tsMrevrange(TimeRange range, MRangeOptions<K, V> options);

	Mono<Sample> tsGet(K key);

	Flux<GetResult<K, V>> tsMget(MGetOptions<K, V> options);

	Flux<GetResult<K, V>> tsMget(V... filters);

	Flux<GetResult<K, V>> tsMgetWithLabels(V... filters);

	Flux<Object> tsInfo(K key);

	Flux<Object> tsInfoDebug(K key);

	Flux<V> tsQueryIndex(V... filters);

	Mono<Long> tsDel(K key, TimeRange range);
}
