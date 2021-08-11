package com.redislabs.lettucemod.search.aggregate;

import com.redislabs.lettucemod.search.AggregateOptions;
import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;
import io.lettuce.core.internal.LettuceAssert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("rawtypes")
public class GroupBy implements AggregateOptions.Operation {

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
    public void build(RediSearchCommandArgs args) {
        args.add(CommandKeyword.GROUPBY);
        args.add(properties.length);
        for (String property : properties) {
            args.addProperty(property);
        }
        for (AggregateOptions.Reducer reducer : reducers) {
            reducer.build(args);
        }
    }

    public static GroupByBuilder property(String property) {
        return properties(property);
    }

    public static GroupByBuilder properties(String... properties) {
        return new GroupByBuilder(properties);
    }

    public static class GroupByBuilder {

        private final List<String> properties = new ArrayList<>();
        private final List<AggregateOptions.Reducer> reducers = new ArrayList<>();

        public GroupByBuilder(String... properties) {
            Collections.addAll(this.properties, properties);
        }

        public GroupByBuilder property(String property) {
            return properties(property);
        }

        public GroupByBuilder reducer(AggregateOptions.Reducer reducer) {
            return reducers(reducer);
        }

        public GroupByBuilder reducers(AggregateOptions.Reducer... reducers) {
            Collections.addAll(this.reducers, reducers);
            return this;
        }

        public GroupBy build() {
            return new GroupBy(properties.toArray(new String[0]), reducers.toArray(new AggregateOptions.Reducer[0]));
        }

    }

}
