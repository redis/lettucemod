package com.redis.lettucemod.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TDigestDoubleListOutput<K,V> extends CommandOutput<K, V, List<Double>> {
    private boolean initialized;
    public TDigestDoubleListOutput(RedisCodec<K, V> codec) {
        super(codec, new ArrayList<>());
    }

    @Override
    public void set(ByteBuffer bytes){
        if(bytes == null){
            output.add(null);
        } else{
            String byteStr = decodeAscii(bytes);
            if(Objects.equals(byteStr, "nan")){
                output.add(Double.NaN);
            } else if (Objects.equals(byteStr, "inf")){
                output.add(Double.POSITIVE_INFINITY);
            } else if (Objects.equals(byteStr, "-inf")){
                output.add(Double.NEGATIVE_INFINITY);
            } else{
                output.add(Double.parseDouble(byteStr));
            }
        }
    }

    @Override
    public void set(double number){
        output.add(number);
    }

    @Override
    public void multi(int count){
        if(!initialized){
            output = OutputFactory.newList(count);
            initialized = true;
        }
    }
}
