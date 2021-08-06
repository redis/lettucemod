package com.redislabs.lettucemod.search.aggregate.reducers;

import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;

public class CountDistinctish extends AbstractPropertyReducer {

    private CountDistinctish(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(RediSearchCommandArgs args) {
        args.add(CommandKeyword.COUNT_DISTINCTISH);
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
