package com.redis.lettucemod.bloom;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

import java.util.Optional;

public class CfReserveOptions implements CompositeArgument {
    Long capacity;
    Optional<Long> bucketSize;
    Optional<Long> maxIterations;
    Optional<Long> expansion;

    private CfReserveOptions(Long capacity, Optional<Long> bucketSize, Optional<Long> maxIterations, Optional<Long> expansion) {
        this.capacity = capacity;
        this.bucketSize = bucketSize;
        this.maxIterations = maxIterations;
        this.expansion = expansion;
    }

    @Override
    public <K, V> void build(CommandArgs<K, V> commandArgs) {
        commandArgs.add(capacity);
        bucketSize.ifPresent(b->{
            commandArgs.add("BUCKETSIZE");
            commandArgs.add(b);
        });

        maxIterations.ifPresent(m->{
            commandArgs.add("MAXITERATIONS");
            commandArgs.add(m);
        });

        expansion.ifPresent(e->{
            commandArgs.add("EXPANSION");
            commandArgs.add(e);
        });
    }

    public static class Builder{
        Long capacity;
        Optional<Long> bucketSize = Optional.empty();
        Optional<Long> maxIterations = Optional.empty();
        Optional<Long> expansion = Optional.empty();

        public Builder(Long capacity) {
            this.capacity = capacity;
        }

        public Builder bucketSize(Long bucketSize){
            this.bucketSize = Optional.of(bucketSize);
            return this;
        }

        public Builder maxIterations(Long maxIterations){
            this.maxIterations = Optional.of(maxIterations);
            return this;
        }

        public Builder expansion(Long expansion){
            this.expansion = Optional.of(expansion);
            return this;
        }

        public CfReserveOptions Build(){return new CfReserveOptions(capacity, bucketSize, maxIterations, expansion);}
    }
}
