package com.redis.lettucemod.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceStrings;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

public class TDigestDoubleOutput<K,V> extends CommandOutput<K,V,Double> {
    public TDigestDoubleOutput(RedisCodec<K,V> codec){
        super(codec, null);
    }

    @Override
    public void set(ByteBuffer bytes){
        if(bytes == null){
            output = null;
        } else{
            output = LettuceStrings.toDouble(decodeString(bytes));
        }
    }

    @Override
    public void set(double number){
        output = number;
    }
}
