package com.redis.lettucemod.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class TopKListWithScoresOutput <K,V> extends CommandOutput<K,V, Map<String, Long>> {
    public TopKListWithScoresOutput(RedisCodec<K, V> codec) {
        super(codec, new HashMap<>());
    }

    String field;

    @Override
    public void set(ByteBuffer buffer){
        field = decodeAscii(buffer);
    }

    @Override
    public void set(long integer) {
        output.put(field,integer);
    }
}
