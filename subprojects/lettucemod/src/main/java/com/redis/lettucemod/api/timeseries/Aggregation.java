package com.redis.lettucemod.api.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;
import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import lombok.AllArgsConstructor;
import lombok.Builder;

@Builder
@AllArgsConstructor
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

    private Type type;
    private long timeBucket;

    @Override
    public <K, V> void build(CommandArgs<K, V> args) {
        args.add(TimeSeriesCommandKeyword.AGGREGATION);
        args.add(type.getName());
        args.add(timeBucket);
    }

}
