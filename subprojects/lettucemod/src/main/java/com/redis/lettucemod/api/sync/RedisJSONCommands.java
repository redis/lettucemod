package com.redis.lettucemod.api.sync;

import com.redis.lettucemod.json.GetOptions;
import io.lettuce.core.KeyValue;
import io.lettuce.core.output.KeyValueStreamingChannel;

import java.util.List;

public interface RedisJSONCommands<K, V> {

    Long del(K key);

    Long del(K key, K path);

    V get(K key, K... paths);

    V get(K key, GetOptions<K, V> options, K... paths);

    List<KeyValue<K, V>> mget(K path, K... keys);

    Long mget(KeyValueStreamingChannel<K, V> channel, K path, K... keys);

    String set(K key, K path, V json);

    String setNX(K key, K path, V json);

    String setXX(K key, K path, V json);

    String type(K key);

    String type(K key, K path);

    V numIncrBy(K key, K path, double number);

    V numMultBy(K key, K path, double number);

    Long strAppend(K key, V json);

    Long strAppend(K key, K path, V json);

    Long strLen(K key);

    Long strLen(K key, K path);

    Long arrAppend(K key, K path, V... jsons);

    Long arrIndex(K key, K path, V scalar);

    Long arrIndex(K key, K path, V scalar, long start);

    Long arrIndex(K key, K path, V scalar, long start, long stop);

    Long arrInsert(K key, K path, long index, V... jsons);

    Long arrLen(K key);

    Long arrLen(K key, K path);

    V arrPop(K key);

    V arrPop(K key, K path);

    V arrPop(K key, K path, long index);

    Long arrTrim(K key, K path, long start, long stop);

    List<K> objKeys(K key);

    List<K> objKeys(K key, K path);

    Long objLen(K key);

    Long objLen(K key, K path);

}
