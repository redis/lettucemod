package com.redislabs.mesclun.search.aggregate;

import com.redislabs.mesclun.search.AggregateOptions;
import com.redislabs.mesclun.search.protocol.CommandKeyword;
import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;

@SuppressWarnings("rawtypes")
public class Apply implements AggregateOptions.Operation {

    private final String expression;
    private final String as;

    public Apply(String expression, String as) {
        this.expression = expression;
        this.as = as;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void build(RediSearchCommandArgs args) {
        args.add(CommandKeyword.APPLY);
        args.add(expression);
        args.add(CommandKeyword.AS);
        args.add(as);
    }

    public static ApplyBuilder expression(String expression) {
        return new ApplyBuilder(expression);
    }

    public static class ApplyBuilder {

        private final String expression;

        public ApplyBuilder(String expression) {
            this.expression = expression;
        }

        public Apply as(String as) {
            return new Apply(expression, as);
        }
    }

}