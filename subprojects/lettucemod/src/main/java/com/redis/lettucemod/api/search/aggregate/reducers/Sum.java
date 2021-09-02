package com.redis.lettucemod.api.search.aggregate.reducers;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;

public class Sum extends AbstractPropertyReducer {

    public Sum(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(SearchCommandArgs args) {
        args.add(SearchCommandKeyword.SUM);
        args.add(1);
        args.addProperty(property);
    }

    public static SumBuilder property(String property) {
        return new SumBuilder(property);
    }

    public static class SumBuilder extends PropertyReducerBuilder<SumBuilder> {

        public SumBuilder(String property) {
            super(property);
        }

        public Sum build() {
            return new Sum(as, property);
        }
    }
}
