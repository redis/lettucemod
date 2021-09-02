package com.redis.lettucemod.api.search.aggregate;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.api.search.AggregateOptions;
import com.redis.lettucemod.protocol.SearchCommandArgs;

public class Filter<K, V> implements AggregateOptions.Operation<K, V> {

    private final V expression;

    public Filter(V expression) {
        this.expression = expression;
    }

    public static <K, V> Filter<K, V> expression(V expression) {
        return new Filter<>(expression);
    }

    @Override
    public void build(SearchCommandArgs<K, V> args) {
        args.add(SearchCommandKeyword.FILTER);
        args.addValue(expression);
    }

}
