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

	Mono<Long> jsonDel(K key, String path);

	@SuppressWarnings("unchecked")
	Mono<V> jsonGet(K key, K... paths);

	@SuppressWarnings("unchecked")
	Mono<V> jsonGet(K key, GetOptions options, K... paths);

	@SuppressWarnings("unchecked")
	Flux<KeyValue<K, V>> jsonMget(String path, K... keys);

	Mono<String> jsonSet(K key, String path, V json);

	Mono<String> jsonSet(K key, String path, V json, SetMode mode);

	Mono<String> jsonMerge(K key, String path, V json);

	Mono<String> jsonType(K key);

	Mono<String> jsonType(K key, String path);

	Mono<V> jsonNumincrby(K key, String path, double number);

	Mono<V> jsonNummultby(K key, String path, double number);

	Mono<Long> jsonStrappend(K key, V json);

	Mono<Long> jsonStrappend(K key, String path, V json);

	Mono<Long> jsonStrlen(K key, String path);

	@SuppressWarnings("unchecked")
	Mono<Long> jsonArrappend(K key, String path, V... jsons);

	Mono<Long> jsonArrindex(K key, String path, V scalar);

	Mono<Long> jsonArrindex(K key, String path, V scalar, Slice slice);

	@SuppressWarnings("unchecked")
	Mono<Long> jsonArrinsert(K key, String path, long index, V... jsons);

	Mono<Long> jsonArrlen(K key);

	Mono<Long> jsonArrlen(K key, String path);

	Mono<V> jsonArrpop(K key);

	Mono<V> jsonArrpop(K key, ArrpopOptions<K> options);

	Mono<Long> jsonArrtrim(K key, String path, long start, long stop);

	Flux<K> jsonObjkeys(K key);

	Flux<K> jsonObjkeys(K key, String path);

	Mono<Long> jsonObjlen(K key);

	Mono<Long> jsonObjlen(K key, String path);

}
