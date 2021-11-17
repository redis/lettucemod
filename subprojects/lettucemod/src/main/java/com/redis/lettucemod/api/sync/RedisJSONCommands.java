package com.redis.lettucemod.api.sync;

import io.lettuce.core.KeyValue;
import io.lettuce.core.output.KeyValueStreamingChannel;

import java.util.List;

import com.redis.lettucemod.json.GetOptions;
import com.redis.lettucemod.json.SetMode;

public interface RedisJSONCommands<K, V> {

    Long jsonDel(K key);

    Long jsonDel(K key, K path);

    @SuppressWarnings("unchecked")
	V jsonGet(K key, K... paths);

    @SuppressWarnings("unchecked")
	V jsonGet(K key, GetOptions options, K... paths);

    @SuppressWarnings("unchecked")
	List<KeyValue<K, V>> jsonMget(K path, K... keys);

    @SuppressWarnings("unchecked")
	Long jsonMget(KeyValueStreamingChannel<K, V> channel, K path, K... keys);

    String jsonSet(K key, K path, V json);

    String jsonSet(K key, K path, V json, SetMode mode);

    String jsonType(K key);

    String jsonType(K key, K path);

    V numincrby(K key, K path, double number);

    V nummultby(K key, K path, double number);

    Long strappend(K key, V json);

    Long strappend(K key, K path, V json);

    Long strlen(K key, K path);

    @SuppressWarnings("unchecked")
	Long arrappend(K key, K path, V... jsons);

    Long arrindex(K key, K path, V scalar);

    Long arrindex(K key, K path, V scalar, long start);

    Long arrindex(K key, K path, V scalar, long start, long stop);

    @SuppressWarnings("unchecked")
	Long arrinsert(K key, K path, long index, V... jsons);

    Long arrlen(K key);

    Long arrlen(K key, K path);

    V arrpop(K key);

    V arrpop(K key, K path);

    V arrpop(K key, K path, long index);

    Long arrtrim(K key, K path, long start, long stop);

    List<K> objkeys(K key);

    List<K> objkeys(K key, K path);

    Long objlen(K key);

    Long objlen(K key, K path);

}
