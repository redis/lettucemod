package com.redis.lettucemod.api.search.aggregate.reducers;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;

public class StdDev extends AbstractPropertyReducer {

    public StdDev(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(SearchCommandArgs args) {
        args.add(SearchCommandKeyword.STDDEV);
        args.add(1);
        args.addProperty(property);
    }

    public static StdDevBuilder property(String property) {
        return new StdDevBuilder(property);
    }

    public static class StdDevBuilder extends PropertyReducerBuilder<StdDevBuilder> {

        public StdDevBuilder(String property) {
            super(property);
        }

        public StdDev build() {
            return new StdDev(as, property);
        }
    }

}
