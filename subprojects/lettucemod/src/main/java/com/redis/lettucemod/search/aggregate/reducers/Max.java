package com.redis.lettucemod.search.aggregate.reducers;

import com.redis.lettucemod.search.protocol.CommandKeyword;
import com.redis.lettucemod.search.protocol.RediSearchCommandArgs;

public class Max extends AbstractPropertyReducer {

    public Max(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(RediSearchCommandArgs args) {
        args.add(CommandKeyword.MAX);
        args.add(1);
        args.addProperty(property);
    }

    public static MaxBuilder property(String property) {
        return new MaxBuilder(property);
    }

    public static class MaxBuilder extends PropertyReducerBuilder<MaxBuilder> {

        public MaxBuilder(String property) {
            super(property);
        }

        public Max build() {
            return new Max(as, property);
        }
    }

}
