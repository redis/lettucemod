package com.redis.lettucemod.api.search.aggregate.reducers;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;

public class Avg extends AbstractPropertyReducer {

    public Avg(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(SearchCommandArgs args) {
        args.add(SearchCommandKeyword.AVG);
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

