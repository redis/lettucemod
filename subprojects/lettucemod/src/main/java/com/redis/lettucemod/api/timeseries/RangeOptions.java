package com.redis.lettucemod.api.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;
import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import lombok.Builder;

@Builder
public class RangeOptions implements CompositeArgument {

    private static final String MIN_TIMESTAMP = "-";
    private static final String MAX_TIMESTAMP = "+";

    private long from;
    private long to;
    private Long count;
    private Aggregation aggregation;

    @Override
    public <K, V> void build(CommandArgs<K, V> args) {
        if (from == 0) {
            args.add(MIN_TIMESTAMP);
        } else {
            args.add(from);
        }
        if (to == 0) {
            args.add(MAX_TIMESTAMP);
        } else {
            args.add(to);
        }
        if (count != null) {
            args.add(TimeSeriesCommandKeyword.COUNT);
            args.add(count);
        }
        if (aggregation != null) {
            aggregation.build(args);
        }
    }

}
