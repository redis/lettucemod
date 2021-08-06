package com.redislabs.mesclun.search.aggregate.reducers;

import com.redislabs.mesclun.search.protocol.CommandKeyword;
import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;

public class ToList extends AbstractPropertyReducer {

    public ToList(String as, String property) {
        super(as, property);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(RediSearchCommandArgs args) {
        args.add(CommandKeyword.TOLIST);
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
