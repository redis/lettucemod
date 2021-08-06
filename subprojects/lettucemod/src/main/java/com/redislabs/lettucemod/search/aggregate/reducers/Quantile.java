package com.redislabs.lettucemod.search.aggregate.reducers;

import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;
import lombok.Setter;
import lombok.experimental.Accessors;

public class Quantile extends AbstractPropertyReducer {

    private final double quantile;

    public Quantile(String as, String property, double quantile) {
        super(as, property);
        this.quantile = quantile;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(RediSearchCommandArgs args) {
        args.add(CommandKeyword.QUANTILE);
        args.add(2);
        args.addProperty(property);
        args.add(quantile);
    }

    public static QuantileBuilder property(String property) {
        return new QuantileBuilder(property);
    }

    @Setter
    @Accessors(fluent = true)
    public static class QuantileBuilder extends PropertyReducerBuilder<QuantileBuilder> {

        private double quantile;

        public QuantileBuilder(String property) {
            super(property);
        }

        public Quantile build() {
            return new Quantile(as, property, quantile);
        }
    }

}
