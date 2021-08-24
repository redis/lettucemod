package com.redis.lettucemod.search;

import com.redis.lettucemod.search.protocol.RediSearchCommandArgs;

public interface RediSearchArgument<K, V> {

    void build(RediSearchCommandArgs<K, V> args);

}
