package com.redislabs.lettucemod.search.aggregate;

import com.redislabs.lettucemod.search.AggregateOptions;
import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;

public class Limit<K, V> implements AggregateOptions.Operation<K, V> {

    private final long offset;
    private final long num;

    public Limit(long offset, long num) {
        this.offset = offset;
        this.num = num;
    }

    @Override
    public void build(RediSearchCommandArgs<K, V> args) {
        args.add(CommandKeyword.LIMIT);
        args.add(offset);
        args.add(num);
    }

    public static <K, V> LimitBuilder<K, V> offset(long offset) {
        return new LimitBuilder<>(offset);
    }

    public static class LimitBuilder<K, V> {

        private final long offset;

        public LimitBuilder(long offset) {
            this.offset = offset;
        }

        public Limit<K, V> num(long num) {
            return new Limit<>(offset, num);
        }
    }

}
