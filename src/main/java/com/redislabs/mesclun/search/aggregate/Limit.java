package com.redislabs.mesclun.search.aggregate;

import com.redislabs.mesclun.search.AggregateOptions;
import com.redislabs.mesclun.search.protocol.CommandKeyword;
import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;

public class Limit implements AggregateOptions.Operation {

    private final long offset;
    private final long num;

    public Limit(long offset, long num) {
        this.offset = offset;
        this.num = num;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void build(RediSearchCommandArgs args) {
        args.add(CommandKeyword.LIMIT);
        args.add(offset);
        args.add(num);
    }

    public static LimitBuilder offset(long offset) {
        return new LimitBuilder(offset);
    }

    public static class LimitBuilder {

        private final long offset;

        public LimitBuilder(long offset) {
            this.offset = offset;
        }

        public Limit num(long num) {
            return new Limit(offset, num);
        }
    }

}