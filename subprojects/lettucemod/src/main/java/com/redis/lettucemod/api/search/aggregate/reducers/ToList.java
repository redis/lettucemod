package com.redis.lettucemod.api.search.aggregate.reducers;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;

public class ToList extends AbstractPropertyReducer {

    public ToList(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(SearchCommandArgs args) {
        args.add(SearchCommandKeyword.TOLIST);
        args.add(1);
        args.addProperty(property);
    }

    public static ToListBuilder property(String property) {
        return new ToListBuilder(property);
    }

    public static class ToListBuilder extends PropertyReducerBuilder<ToListBuilder> {

        public ToListBuilder(String property) {
            super(property);
        }

        public ToList build() {
            return new ToList(as, property);
        }
    }
}
