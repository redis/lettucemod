package com.redislabs.lettucemod.search.aggregate;

import com.redislabs.lettucemod.search.AggregateOptions;
import com.redislabs.lettucemod.search.Order;
import com.redislabs.lettucemod.search.RediSearchArgument;
import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;
import io.lettuce.core.internal.LettuceAssert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SortBy<K, V> implements AggregateOptions.Operation<K, V> {

    private final Property[] properties;
    private final Long max;

    public SortBy(Property[] properties, Long max) {
        LettuceAssert.notEmpty(properties, "At least one property is required");
        LettuceAssert.noNullElements(properties, "Properties must not be null");
        this.properties = properties;
        this.max = max;
    }

    @Override
    public void build(RediSearchCommandArgs<K, V> args) {
        args.add(CommandKeyword.SORTBY);
        args.add((long) properties.length * 2);
        for (Property property : properties) {
            property.build(args);
        }
        if (max != null) {
            args.add(CommandKeyword.MAX);
            args.add(max);
        }
    }

    public static <K, V> SortByBuilder<K, V> property(Property property) {
        return properties(property);
    }

    public static <K, V> SortByBuilder<K, V> properties(Property... properties) {
        return new SortByBuilder<>(properties);
    }

    public static class SortByBuilder<K, V> {

        private final List<Property> properties = new ArrayList<>();
        private Long max;

        public SortByBuilder(Property... properties) {
            Collections.addAll(this.properties, properties);
        }

        public SortByBuilder<K, V> property(Property property) {
            return properties(property);
        }

        public SortByBuilder<K, V> max(long max) {
            this.max = max;
            return this;
        }

        public SortBy<K, V> build() {
            return new SortBy<>(properties.toArray(new Property[0]), max);
        }

    }

    @SuppressWarnings("rawtypes")
    public static class Property implements RediSearchArgument {

        private final String name;
        private final Order order;

        public Property(String name, Order order) {
            LettuceAssert.notNull(name, "Name is required");
            LettuceAssert.notNull(order, "Order is required");
            this.name = name;
            this.order = order;
        }

        @Override
        public void build(RediSearchCommandArgs args) {
            args.addProperty(name);
            args.add(order == Order.ASC ? CommandKeyword.ASC : CommandKeyword.DESC);
        }

        public static PropertyBuilder name(String name) {
            return new PropertyBuilder(name);
        }

        public static class PropertyBuilder {

            private final String name;

            public PropertyBuilder(String name) {
                this.name = name;
            }

            public Property order(Order order) {
                return new Property(name, order);
            }
        }

    }
}
