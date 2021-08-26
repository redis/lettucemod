package com.redis.lettucemod.api.reactive;

import com.redis.lettucemod.json.GetOptions;
import io.lettuce.core.KeyValue;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface RedisJSONReactiveCommands<K, V> {

    Mono<Long> del(K key);

    Mono<Long> del(K key, K path);

    Mono<V> get(K key, K... paths);

    Mono<V> get(K key, GetOptions<K, V> options, K... paths);

    Flux<KeyValue<K, V>> mget(K path, K... keys);

    Mono<String> set(K key, K path, V json);

    Mono<String> setNX(K key, K path, V json);

    Mono<String> setXX(K key, K path, V json);

    Mono<String> type(K key);

    Mono<String> type(K key, K path);

    Mono<V> numIncrBy(K key, K path, double number);

    Mono<V> numMultBy(K key, K path, double number);

    Mono<Long> strAppend(K key, V json);

    Mono<Long> strAppend(K key, K path, V json);

    Mono<Long> strLen(K key);

    Mono<Long> strLen(K key, K path);

    Mono<Long> arrAppend(K key, K path, V... jsons);

    Mono<Long> arrIndex(K key, K path, V scalar);

    Mono<Long> arrIndex(K key, K path, V scalar, long start);

    Mono<Long> arrIndex(K key, K path, V scalar, long start, long stop);

    Mono<Long> arrInsert(K key, K path, long index, V... jsons);

    Mono<Long> arrLen(K key);

    Mono<Long> arrLen(K key, K path);

    Mono<V> arrPop(K key);

    Mono<V> arrPop(K key, K path);

    Mono<V> arrPop(K key, K path, long index);

    Mono<Long> arrTrim(K key, K path, long start, long stop);

    Flux<K> objKeys(K key);

    Flux<K> objKeys(K key, K path);

    Mono<Long> objLen(K key);

    Mono<Long> objLen(K key, K path);


}
