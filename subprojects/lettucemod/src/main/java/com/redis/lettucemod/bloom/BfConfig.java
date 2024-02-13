package com.redis.lettucemod.bloom;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import reactor.util.annotation.Nullable;

public class BfConfig implements CompositeArgument {
    Long capacity;
    Double error;
    Boolean nonScaling = false;
    @Nullable Integer expansion = null;

    BfConfig(Long capacity, Double error, Boolean nonScaling, @Nullable Integer expansion){
        this.capacity = capacity;
        this.error = error;
        this.nonScaling = nonScaling;
        this.expansion = expansion;
    }

    @Override
    public <K, V> void build(CommandArgs<K, V> commandArgs) {
        commandArgs.add(error);
        commandArgs.add(capacity);
        if(expansion != null){
            commandArgs.add("EXPANSION");
            commandArgs.add(expansion);
        }

        if(nonScaling){
            commandArgs.add("NONSCALING");
        }
    }

    public static Builder builder(Long capacity, Double error){
        return new Builder(capacity, error);
    }

    public static class Builder {
        public Builder(Long capacity, Double error) {
            this.capacity = capacity;
            this.error = error;
        }

        private Long capacity;
        private Double error;
        private Boolean nonScaling = false;
        @Nullable private Integer expansion = null;

        public Builder nonScaling(Boolean nonScaling) {
            this.nonScaling = nonScaling;
            return this;
        }

        public Builder expansion(Integer expansion) {
            this.expansion = expansion;
            return this;
        }

        public BfConfig build() {
            return new BfConfig(capacity, error, nonScaling, expansion);
        }
    }
}
