package com.redislabs.mesclun.search;

import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;
import io.lettuce.core.internal.LettuceAssert;
import lombok.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.redislabs.mesclun.search.protocol.CommandKeyword.*;

@SuppressWarnings("unused")
@Data
@Builder
public class AggregateOptions<K, V> implements RediSearchArgument<K, V> {

    @Singular
    private List<String> loads;
    @Singular
    private List<Operation<K, V>> operations;

    @Override
    public void build(RediSearchCommandArgs<K, V> args) {
        if (!loads.isEmpty()) {
            args.add(LOAD);
            args.add(loads.size());
            loads.forEach(args::addProperty);
        }
        operations.forEach(o -> o.build(args));
    }

    @SuppressWarnings("unchecked")
    public static class AggregateOptionsBuilder<K, V> {

        public AggregateOptionsBuilder<K, V> apply(V expression, K alias) {
            operation(Operation.Apply.<K, V>builder().expression(expression).as(alias).build());
            return this;
        }

        public AggregateOptionsBuilder<K, V> filter(V expression) {
            operation((Operation<K, V>) Operation.Filter.of(expression));
            return this;
        }

        public AggregateOptionsBuilder<K, V> groupBy(Collection<String> properties, Operation.GroupBy.Reducer<K>... reducers) {
            // Removed checks to accept GROUPBY 0
            operation((Operation<K, V>) Operation.GroupBy.<K>builder().properties(properties).reducers(Arrays.asList(reducers)).build());
            return this;
        }

        public AggregateOptionsBuilder<K, V> sortBy(Operation.SortBy.Property... properties) {
            LettuceAssert.isTrue(properties.length > 0, "At least one sort-by property is required.");
            operation((Operation<K, V>) Operation.SortBy.builder().properties(Arrays.asList(properties)).build());
            return this;
        }

        public AggregateOptionsBuilder<K, V> sortBy(long max, Operation.SortBy.Property... properties) {
            LettuceAssert.isTrue(properties.length > 0, "At least one sort-by property is required.");
            operation((Operation<K, V>) Operation.SortBy.builder().properties(Arrays.asList(properties)).max(max).build());
            return this;
        }

        public AggregateOptionsBuilder<K, V> limit(long offset, long num) {
            operation((Operation<K, V>) Operation.Limit.builder().offset(offset).num(num).build());
            return this;
        }


    }

    public interface Operation<K, V> extends RediSearchArgument<K, V> {


        @Data
        @Builder
        class Apply<K, V> implements Operation<K, V> {

            private V expression;
            private K as;

            @Override
            public void build(RediSearchCommandArgs<K, V> args) {
                args.add(APPLY);
                args.addValue(expression);
                args.add(AS);
                args.addKey(as);
            }

        }

        @Data
        @AllArgsConstructor
        class Filter<V> implements Operation<Object, V> {

            private V expression;

            public static <V> Filter<V> of(V expression) {
                return new Filter<>(expression);
            }

            @Override
            public void build(RediSearchCommandArgs<Object, V> args) {
                args.add(FILTER);
                args.addValue(expression);
            }

        }

        @Data
        @Builder
        class GroupBy<K> implements Operation<K, Object> {

            @Singular
            private List<String> properties;
            @Singular
            private List<Reducer<K>> reducers;

            @Override
            public void build(RediSearchCommandArgs<K, Object> args) {
                args.add(GROUPBY);
                args.add(properties.size());
                properties.forEach(args::addProperty);
                reducers.forEach(reducer -> reducer.build(args));
            }


            @AllArgsConstructor
            @Setter
            public static abstract class Reducer<K> implements RediSearchArgument<K, Object> {

                private K as;

                @Override
                public void build(RediSearchCommandArgs<K, Object> args) {
                    args.add(REDUCE);
                    buildFunction(args);
                    if (as != null) {
                        args.add(AS);
                        args.addKey(as);
                    }
                }

                protected abstract void buildFunction(RediSearchCommandArgs<K, Object> args);

                public static class Avg<K> extends AbstractPropertyReducer<K> {

                    @Builder
                    private Avg(K as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs<K, Object> args, String property) {
                        args.add(AVG);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                public static class Count<K> extends Reducer<K> {

                    private Count(K as) {
                        super(as);
                    }

                    public static <K, V> Count<K> of(K as) {
                        return new Count<>(as);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs<K, Object> args) {
                        args.add(COUNT);
                        args.add(0);
                    }

                }

                public static class CountDistinct<K> extends AbstractPropertyReducer<K> {

                    @Builder
                    private CountDistinct(K as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs<K, Object> args, String property) {
                        args.add(COUNT_DISTINCT);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                public static class CountDistinctish<K> extends AbstractPropertyReducer<K> {

                    @Builder
                    private CountDistinctish(K as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs<K, Object> args, String property) {
                        args.add(COUNT_DISTINCTISH);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                @Getter
                @Setter
                public static class FirstValue<K> extends AbstractPropertyReducer<K> {

                    private By by;

                    @Builder
                    private FirstValue(K as, String property, By by) {
                        super(as, property);
                        this.by = by;
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs<K, Object> args, String property) {
                        args.add(FIRST_VALUE);
                        args.add(getNumberOfArgs());
                        args.addProperty(property);
                        if (by != null) {
                            args.add(BY);
                            args.addProperty(property);
                            if (by.getOrder() != null) {
                                args.add(by.getOrder() == Order.Asc ? ASC : DESC);
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

                    @Data
                    @Builder
                    public static class By {

                        private String property;
                        private Order order;

                    }
                }

                public static class Max<K> extends AbstractPropertyReducer<K> {

                    @Builder
                    private Max(K as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs<K, Object> args, String property) {
                        args.add(MAX);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                public static class Min<K> extends AbstractPropertyReducer<K> {

                    @Builder
                    private Min(K as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs<K, Object> args, String property) {
                        args.add(MIN);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                @Getter
                @Setter
                public static class Quantile<K> extends AbstractPropertyReducer<K> {

                    private double quantile;

                    @Builder
                    private Quantile(K as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs<K, Object> args, String property) {
                        args.add(QUANTILE);
                        args.add(2);
                        args.addProperty(property);
                        args.add(quantile);
                    }

                }

                @Getter
                @Setter
                public static class RandomSample<K> extends AbstractPropertyReducer<K> {

                    private int size;

                    @Builder
                    private RandomSample(K as, String property, int size) {
                        super(as, property);
                        this.size = size;
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs<K, Object> args, String property) {
                        args.add(RANDOM_SAMPLE);
                        args.add(2);
                        args.addProperty(property);
                        args.add(size);
                    }

                }

                public static class StdDev<K> extends AbstractPropertyReducer<K> {

                    @Builder
                    private StdDev(K as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs<K, Object> args, String property) {
                        args.add(STDDEV);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                public static class Sum<K> extends AbstractPropertyReducer<K> {

                    @Builder
                    private Sum(K as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs<K, Object> args, String property) {
                        args.add(SUM);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                public static class ToList<K> extends AbstractPropertyReducer<K> {

                    @Builder
                    private ToList(K as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs<K, Object> args, String property) {
                        args.add(TOLIST);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

            }

            @Getter
            @Setter
            private abstract static class AbstractPropertyReducer<K> extends Reducer<K> {

                private String property;

                protected AbstractPropertyReducer(K as, String property) {
                    super(as);
                    this.property = property;
                }

                @Override
                protected void buildFunction(RediSearchCommandArgs<K, Object> args) {
                    buildFunction(args, property);
                }

                protected abstract void buildFunction(RediSearchCommandArgs<K, Object> args, String property);

            }
        }

        @Data
        @Builder
        class Limit implements Operation<Object, Object> {

            private long offset;
            private long num;

            @Override
            public void build(RediSearchCommandArgs<Object, Object> args) {
                args.add(LIMIT);
                args.add(offset);
                args.add(num);
            }

        }

        @Data
        @Builder
        class SortBy implements Operation<Object, Object> {

            @Singular
            private List<Property> properties;
            private Long max;

            @Override
            public void build(RediSearchCommandArgs<Object, Object> args) {
                args.add(SORTBY);
                args.add((long) properties.size() * 2);
                properties.forEach(property -> property.build(args));
                if (max != null) {
                    args.add(MAX);
                    args.add(max);
                }

            }

            @Data
            @Builder
            public static class Property implements RediSearchArgument<Object, Object> {

                private String property;
                private Order order;

                @Override
                public void build(RediSearchCommandArgs<Object, Object> args) {
                    args.addProperty(property);
                    args.add(order == Order.Desc ? DESC : ASC);
                }

            }
        }

        enum Order {
            Asc, Desc
        }
    }
}
