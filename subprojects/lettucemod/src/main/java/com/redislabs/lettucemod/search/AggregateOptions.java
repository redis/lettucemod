package com.redislabs.lettucemod.search;

import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.redislabs.lettucemod.search.protocol.CommandKeyword.LOAD;
import static com.redislabs.lettucemod.search.protocol.CommandKeyword.VERBATIM;

public class AggregateOptions<K, V> implements RediSearchArgument<K, V> {

    private final List<Operation<K, V>> operations;
    private final List<String> loads;
    private final boolean verbatim;

    public AggregateOptions(List<Operation<K, V>> operations, List<String> loads, boolean verbatim) {
        this.operations = operations;
        this.loads = loads;
        this.verbatim = verbatim;
    }

    @Override
    public void build(RediSearchCommandArgs<K, V> args) {
        if (verbatim) {
            args.add(VERBATIM);
        }
        if (!loads.isEmpty()) {
            args.add(LOAD);
            args.add(loads.size());
            for (String load : loads) {
                args.addProperty(load);
            }
        }
        for (Operation<K, V> operation : operations) {
            operation.build(args);
        }
    }

    public static <K, V> AggregateOptionsBuilder<K, V> builder() {
        return new AggregateOptionsBuilder<>();
    }

    public static <K, V> AggregateOptionsBuilder<K, V> operation(Operation<K, V> operation) {
        return new AggregateOptionsBuilder<K, V>().operation(operation);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> AggregateOptionsBuilder<K, V> operations(Operation<K, V>... operations) {
        return new AggregateOptionsBuilder<K,V>().operations(operations);
    }

    @Setter
    @Accessors(fluent = true)
    public static class AggregateOptionsBuilder<K, V> {

        private final List<Operation<K, V>> operations = new ArrayList<>();
        private final List<String> loads = new ArrayList<>();
        private boolean verbatim;

        public AggregateOptionsBuilder<K, V> operation(Operation<K, V> operation) {
            this.operations.add(operation);
            return this;
        }

        @SuppressWarnings("unchecked")
        public AggregateOptionsBuilder<K, V> operations(Operation<K, V>... operations) {
            this.operations.addAll(Arrays.asList(operations));
            return this;
        }

        public AggregateOptionsBuilder<K, V> load(String load) {
            this.loads.add(load);
            return this;
        }

        @SuppressWarnings("unused")
        public AggregateOptionsBuilder<K, V> loads(String... loads) {
            this.loads.addAll(Arrays.asList(loads));
            return this;
        }

        public AggregateOptions<K, V> build() {
            return new AggregateOptions<>(operations, loads, verbatim);
        }

    }


    @SuppressWarnings("rawtypes")
    public interface Reducer extends RediSearchArgument {
    }

    public interface Operation<K, V> extends RediSearchArgument<K, V> {
    }
}
