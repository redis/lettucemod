package com.redis.lettucemod.search;

import io.lettuce.core.internal.LettuceAssert;

public abstract class PropertyReducer extends Reducer {

    protected final String property;

    protected PropertyReducer(String as, String property) {
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
