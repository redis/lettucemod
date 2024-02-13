package com.redis.lettucemod.bloom;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import reactor.util.annotation.Nullable;

public class BfInsertOptions implements CompositeArgument {
    @Nullable BfConfig config;
    Boolean noCreate = false;
    public BfInsertOptions(@Nullable BfConfig config, Boolean noCreate) {
        this.config = config;
        this.noCreate = noCreate;
    }

    @Override
    public <K, V> void build(CommandArgs<K, V> commandArgs) {
        if(config != null){
            commandArgs.add("CAPACITY");
            commandArgs.add(config.capacity);
            commandArgs.add("ERROR");
            commandArgs.add(config.error);
            if(config.nonScaling){
                commandArgs.add("NONSCALING");
            }

            if(config.expansion != null){
                commandArgs.add("EXPANSION");
                commandArgs.add(config.expansion);
            }
        }

        if(noCreate){
            commandArgs.add("NOCREATE");
        }
    }

    public static Builder builder(){
        return new Builder();
    }

    public static class Builder{
        @Nullable BfConfig config;
        Boolean noCreate = false;
        public Builder config(BfConfig config){
            this.config = config;
            return this;
        }

        public Builder noCreate(Boolean noCreate){
            this.noCreate = noCreate;
            return this;
        }

        public BfInsertOptions build(){return new BfInsertOptions(config, noCreate);}
    }

}
