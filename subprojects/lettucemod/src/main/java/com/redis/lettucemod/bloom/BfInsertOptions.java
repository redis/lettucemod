package com.redis.lettucemod.bloom;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

import java.util.Optional;

public class BfInsertOptions implements CompositeArgument {
    Optional<BfConfig> config;
    Boolean noCreate;

    private BfInsertOptions(Optional<BfConfig> config, Boolean noCreate){
        this.config = config;
        this.noCreate = noCreate;
    }

    @Override
    public <K, V> void build(CommandArgs<K, V> commandArgs) {
        config.ifPresent(c->{
            commandArgs.add("CAPACITY");
            commandArgs.add(c.capacity);
            commandArgs.add("ERROR");
            commandArgs.add(c.error);
            if(c.nonScaling){
                commandArgs.add("NONSCALING");
            }

            c.expansion.ifPresent(e->{
                commandArgs.add("EXPANSION");
                commandArgs.add(e);
            });
        });

        if(noCreate){
            commandArgs.add("NOCREATE");
        }
    }

    public static Builder builder(){
        return new Builder();
    }

    public static class Builder{
        Optional<BfConfig> config = Optional.empty();
        Boolean noCreate = false;
        public Builder config(BfConfig config){
            this.config = Optional.of(config);
            return this;
        }

        public Builder noCreate(Boolean noCreate){
            this.noCreate = noCreate;
            return this;
        }

        public BfInsertOptions build(){return new BfInsertOptions(config, noCreate);}
    }

}
