package com.redis.lettucemod.api.sync;

import com.redis.lettucemod.api.JsonGetOptions;
import io.lettuce.core.KeyValue;
import io.lettuce.core.output.KeyValueStreamingChannel;

import java.util.List;

public interface RedisJSONCommands<K, V> {

    Long jsonDel(K key, K path);

    V get(K key, JsonGetOptions<K, V> options, K... paths);

    List<KeyValue<K, V>> jsonMget(K path, K... keys);

    Long jsonMget(KeyValueStreamingChannel<K, V> channel, K path, K... keys);

    String set(K key, K path, V json);

    String setNX(K key, K path, V json);

    String setXX(K key, K path, V json);

    String type(K key, K path);

    V numincrby(K key, K path, double number);

    V nummultby(K key, K path, double number);

    Long strappend(K key, V json);

    Long strappend(K key, K path, V json);

    Long strlen(K key);

    Long strlen(K key, K path);

    Long arrappend(K key, K path, V... jsons);

    Long arrindex(K key, K path, V scalar);

    Long arrindex(K key, K path, V scalar, long start);

    Long arrindex(K key, K path, V scalar, long start, long stop);

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
