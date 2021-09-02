package com.redis.lettucemod.api.search.aggregate;

import com.redis.lettucemod.api.search.AggregateOptions;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;

public class Limit<K, V> implements AggregateOptions.Operation<K, V> {

    private final long offset;
    private final long num;

    public Limit(long offset, long num) {
        this.offset = offset;
        this.num = num;
    }

    @Override
    public void build(SearchCommandArgs<K, V> args) {
        args.add(SearchCommandKeyword.LIMIT);
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
