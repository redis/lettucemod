package com.redislabs.lettucemod.search.aggregate;

import com.redislabs.lettucemod.search.AggregateOptions;
import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;

public class Filter implements AggregateOptions.Operation {

    private final String expression;

    public Filter(String expression) {
        this.expression = expression;
    }

    public static Filter expression(String expression) {
        return new Filter(expression);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void build(RediSearchCommandArgs args) {
        args.add(CommandKeyword.FILTER);
        args.add(expression);
    }

}
