package com.redis.lettucemod.bloom;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import reactor.util.annotation.Nullable;

public class TDigestMergeOptions implements CompositeArgument {
    private final @Nullable Long compression;
    private final boolean override;

    TDigestMergeOptions(@Nullable Long compression, boolean override){
        this.compression = compression;
        this.override = override;
    }

    @Override
    public <K, V> void build(CommandArgs<K, V> commandArgs) {
        if(compression != null){
            commandArgs.add("COMPRESSION");
            commandArgs.add(compression);
        }

        if(override){
            commandArgs.add("OVERRIDE");
        }
    }

    public static Builder buidler(){ return new Builder();}
    public static class Builder{
        private @Nullable Long compression;
        private boolean override = false;
        public Builder compression(long compression){
            this.compression = compression;
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
