package com.redislabs.mesclun.search.aggregate.reducers;

import com.redislabs.mesclun.search.protocol.CommandKeyword;
import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;

public class StdDev extends AbstractPropertyReducer {

    public StdDev(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(RediSearchCommandArgs args) {
        args.add(CommandKeyword.STDDEV);
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
