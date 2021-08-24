package com.redis.lettucemod.search.aggregate.reducers;

import com.redis.lettucemod.search.protocol.CommandKeyword;
import com.redis.lettucemod.search.protocol.RediSearchCommandArgs;

public class Min extends AbstractPropertyReducer {

    public Min(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(RediSearchCommandArgs args) {
        args.add(CommandKeyword.MIN);
        args.add(1);
        args.addProperty(property);
    }

    public static MinBuilder property(String property) {
        return new MinBuilder(property);
    }

    public static class MinBuilder extends PropertyReducerBuilder<MinBuilder> {

        public MinBuilder(String property) {
            super(property);
        }

        public Min build() {
            return new Min(as, property);
        }
    }

}
