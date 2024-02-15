package com.redis.lettucemod.output;

import com.redis.lettucemod.bloom.TopKInfo;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

public class TopKInfoOutput <K,V> extends CommandOutput<K,V, TopKInfo> {
    public TopKInfoOutput(RedisCodec<K, V> codec) {
        super(codec, new TopKInfo());
    }

    String field;
    boolean decaySet = false;

    @Override
    public void set(ByteBuffer buffer){
        String str = decodeAscii(buffer);
        if(field != null && field.equals("decay") && ! decaySet){
            output.setDecay(Double.parseDouble(str));
        }else {
            field = str;
        }
    }

    @Override
    public void set(long integer){
        switch (field){
            case "k":
                output.setK(integer);
                break;
            case "width":
                output.setWidth(integer);
                break;
            case "depth":
                output.setDepth(integer);
                break;
        }
    }
}
