package com.redis.lettucemod.api.search.aggregate.reducers;

import com.redis.lettucemod.api.search.AggregateOptions;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;

@SuppressWarnings("rawtypes")
abstract class AbstractReducer implements AggregateOptions.Reducer {

    private final String as;

    protected AbstractReducer(String as) {
        this.as = as;
    }

    @Override
    public void build(SearchCommandArgs args) {
        args.add(SearchCommandKeyword.REDUCE);
        buildFunction(args);
        if (as != null) {
            args.add(SearchCommandKeyword.AS);
            args.add(as);
        }
    }

    protected abstract void buildFunction(SearchCommandArgs args);

    @SuppressWarnings("unchecked")
    public static class ReducerBuilder<B extends ReducerBuilder<B>> {

        protected String as;

        public B as(String as) {
            this.as = as;
            return (B) this;
        }

    }

}
