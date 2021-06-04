package com.redislabs.mesclun;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.BaseRedisCommandBuilder;

public class RedisModulesCommandBuilder<K, V> extends BaseRedisCommandBuilder<K, V> {

    static final String MUST_NOT_BE_NULL = "must not be null";

    protected RedisModulesCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    protected void notNull(Object arg, String name) {
        LettuceAssert.notNull(arg, name + " " + MUST_NOT_BE_NULL);
    }
}
