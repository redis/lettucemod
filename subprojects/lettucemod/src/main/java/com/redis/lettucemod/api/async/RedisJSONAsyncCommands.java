package com.redis.lettucemod.api.async;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.output.KeyValueStreamingChannel;

import java.util.List;

import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.SetMode;

public interface RedisJSONAsyncCommands<K, V> {

    RedisFuture<Long> jsonDel(K key);

    RedisFuture<Long> jsonDel(K key, K path);

    @SuppressWarnings("unchecked")
	RedisFuture<V> jsonGet(K key, K... paths);

    @SuppressWarnings("unchecked")
	RedisFuture<V> jsonGet(K key, GetOptions options, K... paths);

    @SuppressWarnings("unchecked")
	RedisFuture<List<KeyValue<K, V>>> jsonMget(K path, K... keys);

    @SuppressWarnings("unchecked")
	RedisFuture<Long> jsonMget(KeyValueStreamingChannel<K, V> channel, K path, K... keys);

    RedisFuture<String> jsonSet(K key, K path, V json);

    RedisFuture<String> jsonSet(K key, K path, V json, SetMode mode);

    RedisFuture<String> jsonType(K key);

    RedisFuture<String> jsonType(K key, K path);

    RedisFuture<V> jsonNumincrby(K key, K path, double number);

    RedisFuture<V> jsonNummultby(K key, K path, double number);

    RedisFuture<Long> jsonStrappend(K key, V json);

    RedisFuture<Long> jsonStrappend(K key, K path, V json);

    RedisFuture<Long> jsonStrlen(K key, K path);

    @SuppressWarnings("unchecked")
	RedisFuture<Long> jsonArrappend(K key, K path, V... jsons);

    RedisFuture<Long> jsonArrindex(K key, K path, V scalar);

    RedisFuture<Long> jsonArrindex(K key, K path, V scalar, long start);

    RedisFuture<Long> jsonArrindex(K key, K path, V scalar, long start, long stop);

    @SuppressWarnings("unchecked")
	RedisFuture<Long> jsonArrinsert(K key, K path, long index, V... jsons);

    RedisFuture<Long> jsonArrlen(K key);

    RedisFuture<Long> jsonArrlen(K key, K path);

    RedisFuture<V> jsonArrpop(K key);

    RedisFuture<V> jsonArrpop(K key, K path);

    RedisFuture<V> jsonArrpop(K key, K path, long index);

    RedisFuture<Long> jsonArrtrim(K key, K path, long start, long stop);

    RedisFuture<List<K>> jsonObjkeys(K key);

    RedisFuture<List<K>> jsonObjkeys(K key, K path);

    RedisFuture<Long> jsonObjlen(K key);

    RedisFuture<Long> jsonObjlen(K key, K path);

}
