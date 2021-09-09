package com.redis.lettucemod.api.reactive;

import com.redis.lettucemod.api.JsonGetOptions;
import io.lettuce.core.KeyValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RedisJSONReactiveCommands<K, V> {

    Mono<Long> jsonDel(K key, K path);

    Mono<V> get(K key, JsonGetOptions options, K... paths);

    Flux<KeyValue<K, V>> jsonMget(K path, K... keys);

    Mono<String> set(K key, K path, V json);

    Mono<String> setNX(K key, K path, V json);

    Mono<String> setXX(K key, K path, V json);

    Mono<String> type(K key, K path);

    Mono<V> numincrby(K key, K path, double number);

    Mono<V> nummultby(K key, K path, double number);

    Mono<Long> strappend(K key, V json);

    Mono<Long> strappend(K key, K path, V json);

    Mono<Long> strlen(K key, K path);

    Mono<Long> arrappend(K key, K path, V... jsons);

    Mono<Long> arrindex(K key, K path, V scalar);

    Mono<Long> arrindex(K key, K path, V scalar, long start);

    Mono<Long> arrindex(K key, K path, V scalar, long start, long stop);

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
