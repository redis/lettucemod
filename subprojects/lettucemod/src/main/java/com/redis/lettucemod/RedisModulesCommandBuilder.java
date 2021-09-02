package com.redis.lettucemod;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.KeyStreamingChannel;
import io.lettuce.core.output.KeyValueStreamingChannel;
import io.lettuce.core.output.ValueStreamingChannel;
import io.lettuce.core.protocol.BaseRedisCommandBuilder;
import io.lettuce.core.protocol.CommandArgs;

public class RedisModulesCommandBuilder<K, V> extends BaseRedisCommandBuilder<K, V> {

    private static final String MUST_NOT_BE_NULL = "must not be null";
    private static final String MUST_NOT_BE_EMPTY = "must not be empty";

    protected CommandArgs<K, V> args(K key) {
        notNullKey(key);
        return new CommandArgs<>(codec).addKey(key);
    }

    protected RedisModulesCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    protected static void notNull(Object arg, String name) {
        LettuceAssert.notNull(arg, name + " " + MUST_NOT_BE_NULL);
    }

    protected static void notNull(KeyStreamingChannel<?> channel) {
        LettuceAssert.notNull(channel, "KeyValueStreamingChannel " + MUST_NOT_BE_NULL);
    }

    protected static void notNull(ValueStreamingChannel<?> channel) {
        LettuceAssert.notNull(channel, "ValueStreamingChannel " + MUST_NOT_BE_NULL);
    }

    protected static void notNull(KeyValueStreamingChannel<?, ?> channel) {
        LettuceAssert.notNull(channel, "KeyValueStreamingChannel " + MUST_NOT_BE_NULL);
    }

    protected static void notEmptyKeys(Object[] keys) {
        notNull(keys, "Keys");
        LettuceAssert.notEmpty(keys, "Keys " + MUST_NOT_BE_EMPTY);
    }

    protected static void notEmptyValues(Object[] values) {
        notNull(values, "Values");
        LettuceAssert.notEmpty(values, "Values " + MUST_NOT_BE_EMPTY);
    }

    protected static void notEmpty(Object[] array, String name) {
        notNull(array, name);
        LettuceAssert.notEmpty(array, name + " " + MUST_NOT_BE_EMPTY);
    }

    protected static void notNullKey(Object key) {
        notNull(key, "Key");
    }
}
