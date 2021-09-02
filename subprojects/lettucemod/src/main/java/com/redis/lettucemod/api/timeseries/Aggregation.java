package com.redis.lettucemod.api.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;
import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import lombok.Data;

@Data
public class Aggregation implements CompositeArgument {

    public enum Type {

        AVG, SUM, MIN, MAX, RANGE, COUNT, FIRST, LAST, STD_P("STD.P"), STD_S("STD.S"), VAR_P("VAR.P"), VAR_S("VAR.S");

        private final String name;

        public String getName() {
            return name;
        }

        Type(String name) {
            this.name = name;
        }

        Type() {
            this.name = this.name();
        }

    }

    private final Type type;
    private long timeBucket;

    public Aggregation(Type type, long timeBucket) {
        this.type = type;
        this.timeBucket = timeBucket;
    }

    @Override
    public <K, V> void build(CommandArgs<K, V> args) {
        args.add(TimeSeriesCommandKeyword.AGGREGATION);
        args.add(type.getName());
        args.add(timeBucket);
    }

    public static AggregationBuilder type(Type type) {
        return new AggregationBuilder(type);
    }

    public static class AggregationBuilder {

        private final Type type;

        public AggregationBuilder(Type type) {
            this.type = type;
        }

        public Aggregation timeBucket(long timeBucket) {
            return new Aggregation(type, timeBucket);
        }

    }
}
