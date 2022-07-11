package com.redis.lettucemod.search;

public interface RediSearchArgument<K, V> {

    void build(SearchCommandArgs<K, V> args);

}
