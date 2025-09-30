package com.redis.lettucemod.api.async;

import io.lettuce.core.RedisFuture;

import java.util.List;

public interface RediSearchAsyncCommands<K, V> extends io.lettuce.core.api.async.RediSearchAsyncCommands<K, V> {

    RedisFuture<List<Object>> ftInfo(K index);

}
