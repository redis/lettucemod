package com.redis.lettucemod.bloom;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

import java.util.Optional;

public class BfConfig implements CompositeArgument {
    Long capacity;
    Double error;
    Boolean nonScaling;
    Optional<Integer> expansion;

    private BfConfig(Long capacity, Double error, Boolean nonScaling, Optional<Integer> expansion){
        this.capacity = capacity;
        this.error = error;
        this.nonScaling = nonScaling;
        this.expansion = expansion;
    }

    @Override
    public <K, V> void build(CommandArgs<K, V> commandArgs) {
        commandArgs.add(error);
        commandArgs.add(capacity);
        expansion.ifPresent(e->{
            commandArgs.add("EXPANSION");
            commandArgs.add(e);
        });

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

        private final Long capacity;
        private final Double error;
        private Boolean nonScaling = false;
        private Optional<Integer> expansion = Optional.empty();

        public Builder nonScaling(Boolean nonScaling) {
            this.nonScaling = nonScaling;
            return this;
        }

        public Builder expansion(Integer expansion) {
            this.expansion = Optional.of(expansion);
            return this;
        }

        public BfConfig build() {
            return new BfConfig(capacity, error, nonScaling, expansion);
        }
    }
}
