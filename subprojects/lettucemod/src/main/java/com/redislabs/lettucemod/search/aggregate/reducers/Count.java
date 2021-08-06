package com.redislabs.lettucemod.search.aggregate.reducers;

import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;

@SuppressWarnings("rawtypes")
public class Count extends AbstractReducer {

    public Count(String as) {
        super(as);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(RediSearchCommandArgs args) {
        args.add(CommandKeyword.COUNT);
        args.add(0);
    }

    public static Count create() {
        return new Count(null);
    }

    public static Count as(String as) {
        return new Count(as);
    }


}
