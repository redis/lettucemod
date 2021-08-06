package com.redislabs.lettucemod.search.aggregate.reducers;

import com.redislabs.lettucemod.search.AggregateOptions;
import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;

@SuppressWarnings("rawtypes")
abstract class AbstractReducer implements AggregateOptions.Reducer {

    private final String as;

    protected AbstractReducer(String as) {
        this.as = as;
    }

    @Override
    public void build(RediSearchCommandArgs args) {
        args.add(CommandKeyword.REDUCE);
        buildFunction(args);
        if (as != null) {
            args.add(CommandKeyword.AS);
            args.add(as);
        }
    }

    protected abstract void buildFunction(RediSearchCommandArgs args);

    @SuppressWarnings("unchecked")
    public static class ReducerBuilder<B extends ReducerBuilder<B>> {

        protected String as;

        public B as(String as) {
            this.as = as;
            return (B) this;
        }

    }

}
