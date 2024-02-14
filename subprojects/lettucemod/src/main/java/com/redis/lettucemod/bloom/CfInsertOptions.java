package com.redis.lettucemod.bloom;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

import java.util.Optional;

public class CfInsertOptions implements CompositeArgument {
    Optional<Long> capacity;
    Optional<Boolean> noCreate;

    private CfInsertOptions(Optional<Long> capacity, Optional<Boolean> noCreate){
        this.capacity = capacity;
        this.noCreate = noCreate;
    }

    @Override
    public <K, V> void build(CommandArgs<K, V> commandArgs) {
        capacity.ifPresent(c->{
            commandArgs.add("CAPACITY");
            commandArgs.add(c);
        });

        if(noCreate.isPresent() && noCreate.get()){
            commandArgs.add("NOCREATE");
        }
    }

    public static Builder builder(){return new Builder();}

    public static class Builder{
        Optional<Long> capacity = Optional.empty();
        Optional<Boolean> noCreate = Optional.empty();

        public Builder capacity(Long capacity){
            this.capacity = Optional.of(capacity);
            return this;
        }

        public Builder noCreate(Boolean noCreate){
            this.noCreate = Optional.of(noCreate);
            return this;
        }

        public CfInsertOptions build(){return new CfInsertOptions(capacity, noCreate);}
    }
}
