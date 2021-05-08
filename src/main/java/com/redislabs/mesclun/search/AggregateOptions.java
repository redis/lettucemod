package com.redislabs.mesclun.search;

import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;
import io.lettuce.core.internal.LettuceAssert;
import lombok.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.redislabs.mesclun.search.protocol.CommandKeyword.*;

@Data
@Builder
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregateOptions implements RediSearchArgument {

    @Singular
    private List<String> loads;
    @Singular
    private List<Operation> operations;

    @Override
    public void build(RediSearchCommandArgs args) {
        if (!loads.isEmpty()) {
            args.add(LOAD);
            args.add(loads.size());
            loads.forEach(args::addProperty);
        }
        operations.forEach(o -> o.build(args));
    }

    public static AggregateOptionsBuilder builder() {
        return new AggregateOptionsBuilder();
    }

    @SuppressWarnings("unchecked")
    public static class AggregateOptionsBuilder {

        public AggregateOptionsBuilder apply(String expression, String alias) {
            operation(Operation.Apply.builder().expression(expression).as(alias).build());
            return this;
        }

        public AggregateOptionsBuilder filter(String expression) {
            operation(Operation.Filter.of(expression));
            return this;
        }

        public AggregateOptionsBuilder groupBy(Collection<String> properties, Operation.GroupBy.Reducer... reducers) {
            // Removed checks to accept GROUPBY 0
            operation(Operation.GroupBy.builder().properties(properties).reducers(Arrays.asList(reducers)).build());
            return this;
        }

        public AggregateOptionsBuilder sortBy(Operation.SortBy.Property... properties) {
            LettuceAssert.isTrue(properties.length > 0, "At least one sort-by property is required.");
            operation(Operation.SortBy.builder().properties(Arrays.asList(properties)).build());
            return this;
        }

        public AggregateOptionsBuilder sortBy(long max, Operation.SortBy.Property... properties) {
            LettuceAssert.isTrue(properties.length > 0, "At least one sort-by property is required.");
            operation(Operation.SortBy.builder().properties(Arrays.asList(properties)).max(max).build());
            return this;
        }

        public AggregateOptionsBuilder limit(long offset, long num) {
            operation(Operation.Limit.builder().offset(offset).num(num).build());
            return this;
        }


    }

    @SuppressWarnings("rawtypes")
    public interface Operation extends RediSearchArgument {


        @Data
        @Builder
        class Apply implements Operation {

            private String expression;
            private String as;

            @Override
            public void build(RediSearchCommandArgs args) {
                args.add(APPLY);
                args.add(expression);
                args.add(AS);
                args.add(as);
            }

        }

        @Data
        class Filter implements Operation {

            private String expression;

            public Filter(String expression) {
                this.expression = expression;
            }

            public static Filter of(String expression) {
                return new Filter(expression);
            }

            @Override
            public void build(RediSearchCommandArgs args) {
                args.add(FILTER);
                args.add(expression);
            }

        }

        @Data
        @Builder
        class GroupBy implements Operation {

            @Singular
            private List<String> properties;
            @Singular
            private List<Reducer> reducers;

            @Override
            public void build(RediSearchCommandArgs args) {
                args.add(GROUPBY);
                args.add(properties.size());
                properties.forEach(args::addProperty);
                reducers.forEach(reducer -> reducer.build(args));
            }


            @SuppressWarnings("rawtypes")
            public static abstract class Reducer implements RediSearchArgument {

                private final String as;

                protected Reducer(String as) {
                    this.as = as;
                }

                @Override
                public void build(RediSearchCommandArgs args) {
                    args.add(REDUCE);
                    buildFunction(args);
                    if (as != null) {
                        args.add(AS);
                        args.add(as);
                    }
                }

                protected abstract void buildFunction(RediSearchCommandArgs args);

                protected abstract static class AbstractPropertyReducer extends Reducer {

                    protected String property;

                    protected AbstractPropertyReducer(String as, String property) {
                        super(as);
                        this.property = property;
                    }

                }

                public static class Avg extends AbstractPropertyReducer {

                    @Builder
                    private Avg(String as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs args) {
                        args.add(AVG);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                public static class Count extends Reducer {

                    private Count(String as) {
                        super(as);
                    }

                    public static Count of(String as) {
                        return new Count(as);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs args) {
                        args.add(COUNT);
                        args.add(0);
                    }

                }

                public static class CountDistinct extends AbstractPropertyReducer {

                    @Builder
                    private CountDistinct(String as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs args) {
                        args.add(COUNT_DISTINCT);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                public static class CountDistinctish extends AbstractPropertyReducer {

                    @Builder
                    private CountDistinctish(String as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs args) {
                        args.add(COUNT_DISTINCTISH);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                public static class FirstValue extends AbstractPropertyReducer {

                    private By by;

                    @Builder
                    private FirstValue(String as, String property, By by) {
                        super(as, property);
                        this.by = by;
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs args) {
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

                public static class Max extends AbstractPropertyReducer {

                    @Builder
                    private Max(String as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs args) {
                        args.add(MAX);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                public static class Min extends AbstractPropertyReducer {

                    @Builder
                    private Min(String as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs args) {
                        args.add(MIN);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                public static class Quantile extends AbstractPropertyReducer {

                    private double quantile;

                    @Builder
                    private Quantile(String as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs args) {
                        args.add(QUANTILE);
                        args.add(2);
                        args.addProperty(property);
                        args.add(quantile);
                    }

                }

                public static class RandomSample extends AbstractPropertyReducer {

                    private int size;

                    @Builder
                    private RandomSample(String as, String property, int size) {
                        super(as, property);
                        this.size = size;
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs args) {
                        args.add(RANDOM_SAMPLE);
                        args.add(2);
                        args.addProperty(property);
                        args.add(size);
                    }

                }

                public static class StdDev extends AbstractPropertyReducer {

                    @Builder
                    private StdDev(String as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs args) {
                        args.add(STDDEV);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                public static class Sum extends AbstractPropertyReducer {

                    @Builder
                    private Sum(String as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs args) {
                        args.add(SUM);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

                public static class ToList extends AbstractPropertyReducer {

                    @Builder
                    private ToList(String as, String property) {
                        super(as, property);
                    }

                    @Override
                    protected void buildFunction(RediSearchCommandArgs args) {
                        args.add(TOLIST);
                        args.add(1);
                        args.addProperty(property);
                    }

                }

            }

        }

        @Data
        @Builder
        class Limit implements Operation {

            private long offset;
            private long num;

            @Override
            public void build(RediSearchCommandArgs args) {
                args.add(LIMIT);
                args.add(offset);
                args.add(num);
            }

        }

        @Data
        @Builder
        class SortBy implements Operation {

            @Singular
            private List<Property> properties;
            private Long max;

            @Override
            public void build(RediSearchCommandArgs args) {
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
            public static class Property implements RediSearchArgument {

                private String property;
                private Order order;

                @Override
                public void build(RediSearchCommandArgs args) {
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
