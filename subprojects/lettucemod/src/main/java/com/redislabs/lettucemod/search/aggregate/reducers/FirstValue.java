package com.redislabs.lettucemod.search.aggregate.reducers;

import com.redislabs.lettucemod.search.Order;
import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings("rawtypes")
public class FirstValue extends AbstractPropertyReducer {

    private final By by;

    public FirstValue(String as, String property, By by) {
        super(as, property);
        this.by = by;
    }

    @Override
    protected void buildFunction(RediSearchCommandArgs args) {
        args.add(CommandKeyword.FIRST_VALUE);
        args.add(getNumberOfArgs());
        args.addProperty(property);
        if (by != null) {
            args.add(CommandKeyword.BY);
            args.addProperty(property);
            if (by.getOrder() != null) {
                args.add(by.getOrder() == Order.ASC ? CommandKeyword.ASC : CommandKeyword.DESC);
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
