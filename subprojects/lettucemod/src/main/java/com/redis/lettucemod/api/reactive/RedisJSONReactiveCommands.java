package com.redis.lettucemod.api.reactive;

import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.SetMode;

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

    Mono<V> numincrby(K key, K path, double number);

    Mono<V> nummultby(K key, K path, double number);

    Mono<Long> strappend(K key, V json);

    Mono<Long> strappend(K key, K path, V json);

    Mono<Long> strlen(K key, K path);

    @SuppressWarnings("unchecked")
	Mono<Long> arrappend(K key, K path, V... jsons);

    Mono<Long> arrindex(K key, K path, V scalar);

    Mono<Long> arrindex(K key, K path, V scalar, long start);

    Mono<Long> arrindex(K key, K path, V scalar, long start, long stop);

    @SuppressWarnings("unchecked")
	Mono<Long> arrinsert(K key, K path, long index, V... jsons);

    Mono<Long> arrlen(K key);

    Mono<Long> arrlen(K key, K path);

    Mono<V> arrpop(K key);

    Mono<V> arrpop(K key, K path);

    Mono<V> arrpop(K key, K path, long index);

    Mono<Long> arrtrim(K key, K path, long start, long stop);

    Flux<K> objkeys(K key);

    Flux<K> objkeys(K key, K path);

    Mono<Long> objlen(K key);

    Mono<Long> objlen(K key, K path);


}
