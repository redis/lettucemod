package com.redis.lettucemod.api.search.aggregate.reducers;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;

public class CountDistinct extends AbstractPropertyReducer {

    public CountDistinct(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(SearchCommandArgs args) {
        args.add(SearchCommandKeyword.COUNT_DISTINCT);
        args.add(1);
        args.addProperty(property);
    }

    public static CountDistinctBuilder property(String property) {
        return new CountDistinctBuilder(property);
    }

    public static class CountDistinctBuilder extends PropertyReducerBuilder<CountDistinctBuilder> {

        public CountDistinctBuilder(String property) {
            super(property);
        }

        public CountDistinct build() {
            return new CountDistinct(as, property);
        }

    }

}
