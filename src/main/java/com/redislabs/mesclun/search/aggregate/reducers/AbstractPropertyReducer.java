package com.redislabs.mesclun.search.aggregate.reducers;

import io.lettuce.core.internal.LettuceAssert;

abstract class AbstractPropertyReducer extends AbstractReducer {

    protected final String property;

    protected AbstractPropertyReducer(String as, String property) {
        super(as);
        this.property = property;
    }

    public static class PropertyReducerBuilder<B extends PropertyReducerBuilder<B>> extends ReducerBuilder<B> {

        protected final String property;

        protected PropertyReducerBuilder(String property) {
            LettuceAssert.notNull(property, "Property is required");
            this.property = property;
        }

    }

}