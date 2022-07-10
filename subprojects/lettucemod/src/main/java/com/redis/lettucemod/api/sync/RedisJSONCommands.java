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

    V jsonNumincrby(K key, K path, double number);

    V jsonNummultby(K key, K path, double number);

    Long jsonStrappend(K key, V json);

    Long jsonStrappend(K key, K path, V json);

    Long jsonStrlen(K key, K path);

    @SuppressWarnings("unchecked")
	Long jsonArrappend(K key, K path, V... jsons);

    Long jsonArrindex(K key, K path, V scalar);

    Long jsonArrindex(K key, K path, V scalar, long start);

    Long jsonArrindex(K key, K path, V scalar, long start, long stop);

    @SuppressWarnings("unchecked")
	Long jsonArrinsert(K key, K path, long index, V... jsons);

    Long jsonArrlen(K key);

    Long jsonArrlen(K key, K path);

    V jsonArrpop(K key);

    V jsonArrpop(K key, K path);

    V jsonArrpop(K key, K path, long index);

    Long jsonArrtrim(K key, K path, long start, long stop);

    List<K> jsonObjkeys(K key);

    List<K> jsonObjkeys(K key, K path);

    Long jsonObjlen(K key);

    Long jsonObjlen(K key, K path);

}
