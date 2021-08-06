package com.redislabs.mesclun.search;

import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;

public interface RediSearchArgument<K, V> {

    void build(RediSearchCommandArgs<K, V> args);

}
