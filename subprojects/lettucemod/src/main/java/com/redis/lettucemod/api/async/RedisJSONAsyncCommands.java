package com.redis.lettucemod.api.async;

import com.redis.lettucemod.json.GetOptions;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.output.KeyValueStreamingChannel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public interface RedisJSONAsyncCommands<K, V> {

    RedisFuture<Long> del(K key);

    RedisFuture<Long> del(K key, K path);

    RedisFuture<V> get(K key, K... paths);

    RedisFuture<V> get(K key, GetOptions<K, V> options, K... paths);

    RedisFuture<List<KeyValue<K, V>>> mget(K path, K... keys);

    RedisFuture<Long> mget(KeyValueStreamingChannel<K, V> channel, K path, K... keys);

    RedisFuture<String> set(K key, K path, V json);

    RedisFuture<String> setNX(K key, K path, V json);

    RedisFuture<String> setXX(K key, K path, V json);

    RedisFuture<String> type(K key);

    RedisFuture<String> type(K key, K path);

    RedisFuture<V> numIncrBy(K key, K path, double number);

    RedisFuture<V> numMultBy(K key, K path, double number);

    RedisFuture<Long> strAppend(K key, V json);

    RedisFuture<Long> strAppend(K key, K path, V json);

    RedisFuture<Long> strLen(K key);

    RedisFuture<Long> strLen(K key, K path);

    RedisFuture<Long> arrAppend(K key, K path, V... jsons);

    RedisFuture<Long> arrIndex(K key, K path, V scalar);

    RedisFuture<Long> arrIndex(K key, K path, V scalar, long start);

    RedisFuture<Long> arrIndex(K key, K path, V scalar, long start, long stop);

    RedisFuture<Long> arrInsert(K key, K path, long index, V... jsons);

    RedisFuture<Long> arrLen(K key);

    RedisFuture<Long> arrLen(K key, K path);

    RedisFuture<V> arrPop(K key);

    RedisFuture<V> arrPop(K key, K path);

    RedisFuture<V> arrPop(K key, K path, long index);

    RedisFuture<Long> arrTrim(K key, K path, long start, long stop);

    RedisFuture<List<K>> objKeys(K key);

    RedisFuture<List<K>> objKeys(K key, K path);

    RedisFuture<Long> objLen(K key);

    RedisFuture<Long> objLen(K key, K path);

}
