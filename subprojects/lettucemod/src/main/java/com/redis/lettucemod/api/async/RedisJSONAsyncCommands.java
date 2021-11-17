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

    RedisFuture<V> numincrby(K key, K path, double number);

    RedisFuture<V> nummultby(K key, K path, double number);

    RedisFuture<Long> strappend(K key, V json);

    RedisFuture<Long> strappend(K key, K path, V json);

    RedisFuture<Long> strlen(K key, K path);

    @SuppressWarnings("unchecked")
	RedisFuture<Long> arrappend(K key, K path, V... jsons);

    RedisFuture<Long> arrindex(K key, K path, V scalar);

    RedisFuture<Long> arrindex(K key, K path, V scalar, long start);

    RedisFuture<Long> arrindex(K key, K path, V scalar, long start, long stop);

    @SuppressWarnings("unchecked")
	RedisFuture<Long> arrinsert(K key, K path, long index, V... jsons);

    RedisFuture<Long> arrlen(K key);

    RedisFuture<Long> arrlen(K key, K path);

    RedisFuture<V> arrpop(K key);

    RedisFuture<V> arrpop(K key, K path);

    RedisFuture<V> arrpop(K key, K path, long index);

    RedisFuture<Long> arrtrim(K key, K path, long start, long stop);

    RedisFuture<List<K>> objkeys(K key);

    RedisFuture<List<K>> objkeys(K key, K path);

    RedisFuture<Long> objlen(K key);

    RedisFuture<Long> objlen(K key, K path);

}
