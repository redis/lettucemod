package com.redis.lettucemod.api.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;

public interface RediSearchArgument<K, V> {

    void build(SearchCommandArgs<K, V> args);

}
