package com.redislabs.lettucemod.search.aggregate;

import com.redislabs.lettucemod.search.AggregateOptions;
import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;

@SuppressWarnings("rawtypes")
public class Filter<V> implements AggregateOptions.Operation {

    private final V expression;

    public Filter(V expression) {
        this.expression = expression;
    }

    public static <V> Filter<V> expression(V expression) {
        return new Filter<>(expression);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void build(RediSearchCommandArgs args) {
        args.add(CommandKeyword.FILTER);
        args.addValue(expression);
    }

}
