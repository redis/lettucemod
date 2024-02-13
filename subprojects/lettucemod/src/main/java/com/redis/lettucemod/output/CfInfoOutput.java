package com.redis.lettucemod.output;

import com.redis.lettucemod.bloom.CfInfo;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

public class CfInfoOutput<K,V> extends CommandOutput<K,V, CfInfo> {
    private String field;
    public CfInfoOutput(RedisCodec<K, V> codec) {
        super(codec, new CfInfo());
    }

    @Override
    public void set(ByteBuffer buffer) { field = decodeAscii(buffer);}

    @Override
    public void set(long integer){
        switch (field){
            case "Size":
                output.setSize(integer);
                break;
            case "Number of buckets":
                output.setNumBuckets(integer);
                break;
            case "Number of filter":
                output.setNumFilters(integer);
                break;
            case "Number of items inserted":
                output.setNumItemsInserted(integer);
                break;
            case "Number of items deleted":
                output.setNumItemsDeleted(integer);
                break;
            case "Bucket size":
                output.setBucketSize(integer);
                break;
            case "Expansion rate":
                output.setExpansionRate(integer);
                break;
            case "Max iteration":
                output.setMaxIteration(integer);
                break;
        }
    }
}
