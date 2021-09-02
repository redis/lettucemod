package com.redis.lettucemod.api.search.aggregate.reducers;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;

public class CountDistinctish extends AbstractPropertyReducer {

    private CountDistinctish(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(SearchCommandArgs args) {
        args.add(SearchCommandKeyword.COUNT_DISTINCTISH);
        args.add(1);
        args.addProperty(property);
    }

    public static CountDistinctishBuilder property(String property) {
        return new CountDistinctishBuilder(property);
    }

    public static class CountDistinctishBuilder extends PropertyReducerBuilder<CountDistinctishBuilder> {

        public CountDistinctishBuilder(String property) {
            super(property);
        }

        public CountDistinctish build() {
            return new CountDistinctish(as, property);
        }
    }

}
