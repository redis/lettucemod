package com.redis.lettucemod.api.search.aggregate;

import com.redis.lettucemod.api.search.AggregateOptions;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;

public class Apply<K, V> implements AggregateOptions.Operation<K, V> {

    private final V expression;
    private final K as;

    public Apply(V expression, K as) {
        this.expression = expression;
        this.as = as;
    }

    @Override
    public void build(SearchCommandArgs<K, V> args) {
        args.add(SearchCommandKeyword.APPLY);
        args.addValue(expression);
        args.add(SearchCommandKeyword.AS);
        args.addKey(as);
    }

    public static <K, V> ApplyBuilder<K, V> expression(V expression) {
        return new ApplyBuilder<>(expression);
    }

    public static class ApplyBuilder<K, V> {

        private final V expression;

        public ApplyBuilder(V expression) {
            this.expression = expression;
        }

        public Apply<K, V> as(K as) {
            return new Apply<>(expression, as);
        }
    }

}
