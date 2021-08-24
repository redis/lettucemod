package com.redis.lettucemod.search.aggregate.reducers;

import com.redis.lettucemod.search.protocol.CommandKeyword;
import com.redis.lettucemod.search.protocol.RediSearchCommandArgs;

public class Avg extends AbstractPropertyReducer {

    public Avg(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(RediSearchCommandArgs args) {
        args.add(CommandKeyword.AVG);
        args.add(1);
        args.addProperty(property);
    }

    public static AvgBuilder property(String property) {
        return new AvgBuilder(property);
    }

    public static class AvgBuilder extends AbstractPropertyReducer.PropertyReducerBuilder<AvgBuilder> {

        public AvgBuilder(String property) {
            super(property);
        }

        public Avg build() {
            return new Avg(as, property);
        }
    }

}

