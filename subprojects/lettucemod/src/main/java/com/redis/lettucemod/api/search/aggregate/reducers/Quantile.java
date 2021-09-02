package com.redis.lettucemod.api.search.aggregate.reducers;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;
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
    protected void buildFunction(SearchCommandArgs args) {
        args.add(SearchCommandKeyword.QUANTILE);
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
