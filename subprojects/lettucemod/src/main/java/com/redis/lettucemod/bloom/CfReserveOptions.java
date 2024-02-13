package com.redis.lettucemod.bloom;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import reactor.util.annotation.Nullable;

public class CfReserveOptions implements CompositeArgument {
    Long capacity;
    @Nullable Long bucketSize;
    @Nullable Long maxIterations;
    @Nullable Long expansion;

    private CfReserveOptions(Long capacity, @Nullable Long bucketSize, @Nullable Long maxIterations, @Nullable Long expansion) {
        this.capacity = capacity;
        this.bucketSize = bucketSize;
        this.maxIterations = maxIterations;
        this.expansion = expansion;
    }

    @Override
    public <K, V> void build(CommandArgs<K, V> commandArgs) {
        commandArgs.add(capacity);
        if(bucketSize != null){
            commandArgs.add("BUCKETSIZE");
            commandArgs.add(bucketSize);
        }

        if(maxIterations != null){
            commandArgs.add("MAXITERATIONS");
            commandArgs.add(maxIterations);
        }

        if(expansion != null){
            commandArgs.add("EXPANSION");
            commandArgs.add(expansion);
        }
    }

    public static class Builder{
        Long capacity;
        @Nullable Long bucketSize;
        @Nullable Long maxIterations;
        @Nullable Long expansion;

        public Builder(Long capacity) {
            this.capacity = capacity;
        }

        public Builder bucketSize(Long bucketSize){
            this.bucketSize = bucketSize;
            return this;
        }

        public Builder maxIterations(Long maxIterations){
            this.maxIterations = maxIterations;
            return this;
        }

        public Builder expansion(Long expansion){
            this.expansion = expansion;
            return this;
        }

        public CfReserveOptions Build(){return new CfReserveOptions(capacity, bucketSize, maxIterations, expansion);}
    }
}
