package com.redis.lettucemod.api.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;
import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RangeOptions implements CompositeArgument {

    private static final String MIN_TIMESTAMP = "-";
    private static final String MAX_TIMESTAMP = "+";
    private Long from;
    private Long to;
    private Long count;
    private Aggregation aggregation;

    @Override
    public <K, V> void build(CommandArgs<K, V> args) {
        if (from == null) {
            args.add(MIN_TIMESTAMP);
        } else {
            args.add(from);
        }
        if (to == null) {
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

    public static RangeBuilder from(long from) {
        return new RangeBuilder().from(from);
    }

    public static RangeBuilder to(long to) {
        return new RangeBuilder().to(to);
    }

    public static RangeBuilder builder() {
        return new RangeBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class RangeBuilder {

        private Long from;
        private Long to;
        private Long count;
        private Aggregation aggregation;

        public RangeOptions build() {
            return new RangeOptions(from, to, count, aggregation);
        }

    }

}
