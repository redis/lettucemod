package com.redislabs.lettucemod.search.aggregate;

import com.redislabs.lettucemod.search.AggregateOptions;
import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;

public class Apply<K, V> implements AggregateOptions.Operation<K, V> {

    private final V expression;
    private final K as;

    public Apply(V expression, K as) {
        this.expression = expression;
        this.as = as;
    }

    @Override
    public void build(RediSearchCommandArgs<K, V> args) {
        args.add(CommandKeyword.APPLY);
        args.addValue(expression);
        args.add(CommandKeyword.AS);
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
