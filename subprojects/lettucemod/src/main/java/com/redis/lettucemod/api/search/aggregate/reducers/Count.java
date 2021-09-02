package com.redis.lettucemod.api.search.aggregate.reducers;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;

@SuppressWarnings("rawtypes")
public class Count extends AbstractReducer {

    public Count(String as) {
        super(as);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(SearchCommandArgs args) {
        args.add(SearchCommandKeyword.COUNT);
        args.add(0);
    }

    public static Count create() {
        return new Count(null);
    }

    public static Count as(String as) {
        return new Count(as);
    }


}
