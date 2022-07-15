package com.redis.lettucemod.api.reactive;

import com.redis.lettucemod.json.ArrpopOptions;
import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.SetMode;
import com.redis.lettucemod.json.Slice;

import io.lettuce.core.KeyValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RedisJSONReactiveCommands<K, V> {

	Mono<Long> jsonDel(K key);

	Mono<Long> jsonDel(K key, K path);

	@SuppressWarnings("unchecked")
	Mono<V> jsonGet(K key, K... paths);

	@SuppressWarnings("unchecked")
	Mono<V> jsonGet(K key, GetOptions options, K... paths);

	@SuppressWarnings("unchecked")
	Flux<KeyValue<K, V>> jsonMget(K path, K... keys);

	Mono<String> jsonSet(K key, K path, V json);

	Mono<String> jsonSet(K key, K path, V json, SetMode mode);

	Mono<String> jsonType(K key);

	Mono<String> jsonType(K key, K path);

	Mono<V> jsonNumincrby(K key, K path, double number);

	Mono<V> jsonNummultby(K key, K path, double number);

	Mono<Long> jsonStrappend(K key, V json);

	Mono<Long> jsonStrappend(K key, K path, V json);

	Mono<Long> jsonStrlen(K key, K path);

	@SuppressWarnings("unchecked")
	Mono<Long> jsonArrappend(K key, K path, V... jsons);

	Mono<Long> jsonArrindex(K key, K path, V scalar);

	Mono<Long> jsonArrindex(K key, K path, V scalar, Slice slice);

	@SuppressWarnings("unchecked")
	Mono<Long> jsonArrinsert(K key, K path, long index, V... jsons);

	Mono<Long> jsonArrlen(K key);

	Mono<Long> jsonArrlen(K key, K path);

	Mono<V> jsonArrpop(K key);

	Mono<V> jsonArrpop(K key, ArrpopOptions<K> options);

	Mono<Long> jsonArrtrim(K key, K path, long start, long stop);

	Flux<K> jsonObjkeys(K key);

	Flux<K> jsonObjkeys(K key, K path);

	Mono<Long> jsonObjlen(K key);

	Mono<Long> jsonObjlen(K key, K path);

}
