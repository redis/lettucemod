package com.redis.lettucemod.api.async;

import java.util.List;

import com.redis.lettucemod.json.ArrpopOptions;
import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.SetMode;
import com.redis.lettucemod.json.Slice;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.output.KeyValueStreamingChannel;

public interface RedisJSONAsyncCommands<K, V> {

	RedisFuture<Long> jsonDel(K key);

	RedisFuture<Long> jsonDel(K key, String path);

	@SuppressWarnings("unchecked")
	RedisFuture<V> jsonGet(K key, K... paths);

	@SuppressWarnings("unchecked")
	RedisFuture<V> jsonGet(K key, GetOptions options, K... paths);

	@SuppressWarnings("unchecked")
	RedisFuture<List<KeyValue<K, V>>> jsonMget(String path, K... keys);

	@SuppressWarnings("unchecked")
	RedisFuture<Long> jsonMget(KeyValueStreamingChannel<K, V> channel, String path, K... keys);

	RedisFuture<String> jsonSet(K key, String path, V json);

	RedisFuture<String> jsonSet(K key, String path, V json, SetMode mode);

	RedisFuture<String> jsonType(K key);

	RedisFuture<String> jsonType(K key, String path);

	RedisFuture<V> jsonNumincrby(K key, String path, double number);

	RedisFuture<V> jsonNummultby(K key, String path, double number);

	RedisFuture<Long> jsonStrappend(K key, V json);

	RedisFuture<Long> jsonStrappend(K key, String path, V json);

	RedisFuture<Long> jsonStrlen(K key, String path);

	@SuppressWarnings("unchecked")
	RedisFuture<Long> jsonArrappend(K key, String path, V... jsons);

	RedisFuture<Long> jsonArrindex(K key, String path, V scalar);

	RedisFuture<Long> jsonArrindex(K key, String path, V scalar, Slice slice);

	@SuppressWarnings("unchecked")
	RedisFuture<Long> jsonArrinsert(K key, String path, long index, V... jsons);

	RedisFuture<Long> jsonArrlen(K key);

	RedisFuture<Long> jsonArrlen(K key, String path);

	RedisFuture<V> jsonArrpop(K key);

	RedisFuture<V> jsonArrpop(K key, ArrpopOptions<K> options);

	RedisFuture<Long> jsonArrtrim(K key, String path, long start, long stop);

	RedisFuture<List<K>> jsonObjkeys(K key);

	RedisFuture<List<K>> jsonObjkeys(K key, String path);

	RedisFuture<Long> jsonObjlen(K key);

	RedisFuture<Long> jsonObjlen(K key, String path);

}
