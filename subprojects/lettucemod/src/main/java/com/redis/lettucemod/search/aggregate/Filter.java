package com.redis.lettucemod.search.aggregate;

import com.redis.lettucemod.search.protocol.CommandKeyword;
import com.redis.lettucemod.search.AggregateOptions;
import com.redis.lettucemod.search.protocol.RediSearchCommandArgs;

public class Filter<K, V> implements AggregateOptions.Operation<K, V> {

    private final V expression;

    public Filter(V expression) {
        this.expression = expression;
    }

    public static <K, V> Filter<K, V> expression(V expression) {
        return new Filter<>(expression);
    }

    @Override
    public void build(RediSearchCommandArgs<K, V> args) {
        args.add(CommandKeyword.FILTER);
        args.addValue(expression);
    }

}
