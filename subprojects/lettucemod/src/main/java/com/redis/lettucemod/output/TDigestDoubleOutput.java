package com.redis.lettucemod.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.DoubleOutput;

import java.nio.ByteBuffer;
import java.util.Objects;

public class TDigestDoubleOutput<K,V> extends CommandOutput<K,V,Double> {
    public TDigestDoubleOutput(RedisCodec<K,V> codec){
        super(codec, null);
    }

    @Override
    public void set(ByteBuffer bytes){
        if(bytes == null){
            output = null;
        } else{
            String byteStr = decodeAscii(bytes);
            if(Objects.equals(byteStr, "nan")){
                output = Double.NaN;
            } else if (Objects.equals(byteStr, "inf")){
                output = Double.POSITIVE_INFINITY;
            } else if (Objects.equals(byteStr, "-inf")){
                output = Double.NEGATIVE_INFINITY;
            } else{
                output = Double.parseDouble(byteStr);
            }
        }
    }

    @Override
    public void set(double number){
        output = number;
    }
}
