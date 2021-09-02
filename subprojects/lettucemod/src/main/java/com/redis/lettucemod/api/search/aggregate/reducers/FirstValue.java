package com.redis.lettucemod.api.search.aggregate.reducers;

import com.redis.lettucemod.api.search.Order;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.protocol.SearchCommandArgs;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class FirstValue extends AbstractPropertyReducer {

    private final By by;

    public FirstValue(String as, String property, By by) {
        super(as, property);
        this.by = by;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void buildFunction(SearchCommandArgs args) {
        args.add(SearchCommandKeyword.FIRST_VALUE);
        args.add(getNumberOfArgs());
        args.addProperty(property);
        if (by != null) {
            args.add(SearchCommandKeyword.BY);
            args.addProperty(property);
            if (by.getOrder() != null) {
                args.add(by.getOrder() == Order.ASC ? SearchCommandKeyword.ASC : SearchCommandKeyword.DESC);
            }
        }
    }

    private int getNumberOfArgs() {
        int nargs = 1;
        if (by != null) {
            nargs += by.getOrder() == null ? 2 : 3;
        }
        return nargs;
    }

    public static FirstValueBuilder property(String property) {
        return new FirstValueBuilder(property);
    }

    @Setter
    @Accessors(fluent = true)
    public static class FirstValueBuilder extends PropertyReducerBuilder<FirstValueBuilder> {

        private By by;

        protected FirstValueBuilder(String property) {
            super(property);
        }

        public FirstValue build() {
            return new FirstValue(as, property, by);
        }
    }

    @Getter
    public static class By {

        private final String property;
        private final Order order;

        public By(String property, Order order) {
            this.property = property;
            this.order = order;
        }

        public static ByBuilder property(String property) {
            return new ByBuilder(property);
        }

        @Setter
        @Accessors(fluent = true)
        public static class ByBuilder {

            private final String property;
            private Order order;

            public ByBuilder(String property) {
                this.property = property;
            }

            public By build() {
                return new By(property, order);
            }

        }

    }
}
