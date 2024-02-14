package com.redis.lettucemod.bloom;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import java.util.Optional;

public class TDigestMergeOptions implements CompositeArgument {
    private final Optional<Long> compression;
    private final boolean override;

    private TDigestMergeOptions(Optional<Long> compression, boolean override){
        this.compression = compression;
        this.override = override;
    }

    @Override
    public <K, V> void build(CommandArgs<K, V> commandArgs) {
        compression.ifPresent(c->{
            commandArgs.add("COMPRESSION");
            commandArgs.add(c);
        });

        if(override){
            commandArgs.add("OVERRIDE");
        }
    }

    public static Builder buidler(){ return new Builder();}
    public static class Builder{
        private Optional<Long> compression = Optional.empty();
        private boolean override = false;
        public Builder compression(long compression){
            this.compression = Optional.of(compression);
            return this;
        }

        public Builder override(boolean override){
            this.override = override;
            return this;
        }

        public TDigestMergeOptions Build(){
            return new TDigestMergeOptions(compression, override);
        }
    }
}
