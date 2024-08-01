package com.redis.lettucemod.json;

import com.redis.lettucemod.protocol.JsonCommandKeyword;
import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;


public enum SetMode implements CompositeArgument {

    NX(JsonCommandKeyword.NX), XX(JsonCommandKeyword.XX);

    private JsonCommandKeyword keyword;


    SetMode(JsonCommandKeyword keyword) {
        this.keyword = keyword;
    }

    @Override
    public <K, V> void build(CommandArgs<K, V> args) {
        args.add(keyword);
    }

}
