package com.redis.lettucemod.json;

import io.lettuce.core.protocol.CommandArgs;

public interface RedisJSONArgument<K, V> {

    void build(CommandArgs<K, V> args);

}
