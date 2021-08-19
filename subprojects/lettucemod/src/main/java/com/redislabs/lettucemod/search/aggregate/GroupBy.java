package com.redislabs.lettucemod.search.aggregate;

import com.redislabs.lettucemod.search.AggregateOptions;
import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;
import io.lettuce.core.internal.LettuceAssert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroupBy<K, V> implements AggregateOptions.Operation<K, V> {

    private final String[] properties;
    private final AggregateOptions.Reducer[] reducers;

    public GroupBy(String[] properties, AggregateOptions.Reducer[] reducers) {
        LettuceAssert.notEmpty(properties, "Group must have at least one property");
        LettuceAssert.noNullElements(properties, "Properties must not be null");
        LettuceAssert.notEmpty(reducers, "Group must have at least one reducer");
        LettuceAssert.noNullElements(reducers, "Reducers must not be null");
        this.properties = properties;
        this.reducers = reducers;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void build(RediSearchCommandArgs<K, V> args) {
        args.add(CommandKeyword.GROUPBY);
        args.add(properties.length);
        for (String property : properties) {
            args.addProperty(property);
        }
        for (AggregateOptions.Reducer reducer : reducers) {
            reducer.build(args);
        }
    }

    public static <K, V> GroupByBuilder<K, V> property(String property) {
        return properties(property);
    }

    public static <K,V> GroupByBuilder<K, V> properties(String... properties) {
        return new GroupByBuilder<>(properties);
    }

    public static class GroupByBuilder<K, V> {

        private final List<String> properties = new ArrayList<>();
        private final List<AggregateOptions.Reducer> reducers = new ArrayList<>();

        public GroupByBuilder(String... properties) {
            Collections.addAll(this.properties, properties);
        }

        public GroupByBuilder<K, V> property(String property) {
            return new GroupByBuilder<>(property);
        }

        public GroupByBuilder<K, V> reducer(AggregateOptions.Reducer reducer) {
            return reducers(reducer);
        }

        public GroupByBuilder<K, V> reducers(AggregateOptions.Reducer... reducers) {
            Collections.addAll(this.reducers, reducers);
            return this;
        }

        public GroupBy<K, V> build() {
            return new GroupBy<>(properties.toArray(new String[0]), reducers.toArray(new AggregateOptions.Reducer[0]));
        }

    }

}
