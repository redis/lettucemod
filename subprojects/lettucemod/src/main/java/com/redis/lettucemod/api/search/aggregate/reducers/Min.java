package com.redis.lettucemod.api.search.aggregate.reducers;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;

public class Min extends AbstractPropertyReducer {

    public Min(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(SearchCommandArgs args) {
        args.add(SearchCommandKeyword.MIN);
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
