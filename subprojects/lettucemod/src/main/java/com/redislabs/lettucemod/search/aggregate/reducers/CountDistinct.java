package com.redislabs.lettucemod.search.aggregate.reducers;

import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;

@SuppressWarnings("rawtypes")
public class CountDistinct extends AbstractPropertyReducer {

    public CountDistinct(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(RediSearchCommandArgs args) {
        args.add(CommandKeyword.COUNT_DISTINCT);
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
