package com.redislabs.lettucemod.search;

import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;

public interface RediSearchArgument<K, V> {

    void build(RediSearchCommandArgs<K, V> args);

}
