package com.redis.lettucemod.bloom;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import reactor.util.annotation.Nullable;

public class CfInsertOptions implements CompositeArgument {
    @Nullable Long capacity;
    @Nullable Boolean noCreate;

    private CfInsertOptions(@Nullable Long capacity, @Nullable Boolean noCreate){
        this.capacity = capacity;
    }

    @Override
    public <K, V> void build(CommandArgs<K, V> commandArgs) {
        if(capacity != null){
            commandArgs.add("CAPACITY");
            commandArgs.add(capacity);
        }

        if(noCreate != null && noCreate){
            commandArgs.add("NOCREATE");
        }
    }

    public static Builder builder(){return new Builder();}

    public static class Builder{
        @Nullable Long capacity;
        @Nullable Boolean noCreate;

        public Builder capacity(Long capacity){
            this.capacity = capacity;
            return this;
        }

        public Builder noCreate(Boolean noCreate){
            this.noCreate = noCreate;
            return this;
        }

        public CfInsertOptions build(){return new CfInsertOptions(capacity, noCreate);}
    }
}
