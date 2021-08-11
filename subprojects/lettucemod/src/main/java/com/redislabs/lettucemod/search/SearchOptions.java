package com.redislabs.lettucemod.search;

import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Singular;
import lombok.experimental.Accessors;

import java.util.List;

@SuppressWarnings("unchecked")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SearchOptions<K, V> implements RediSearchArgument<K, V> {

    private boolean noContent;
    private boolean verbatim;
    private boolean noStopWords;
    private boolean withScores;
    private boolean withPayloads;
    private boolean withSortKeys;
    @Singular
    private List<NumericFilter<K>> filters;
    private GeoFilter<K> geoFilter;
    @Singular
    private List<K> inKeys;
    @Singular
    private List<K> inFields;
    @Singular
    private List<K> returnFields;
    private Summarize<K, V> summarize;
    private Highlight<K, V> highlight;
    private Long slop;
    private boolean inOrder;
    private Language language;
    private String expander;
    private String scorer;
    private V payload;
    private SortBy<K> sortBy;
    private Limit limit;

    @Override
    public void build(RediSearchCommandArgs<K, V> args) {
        if (noContent) {
            args.add(CommandKeyword.NOCONTENT);
        }
        if (verbatim) {
            args.add(CommandKeyword.VERBATIM);
        }
        if (noStopWords) {
            args.add(CommandKeyword.NOSTOPWORDS);
        }
        if (withScores) {
            args.add(CommandKeyword.WITHSCORES);
        }
        if (withPayloads) {
            args.add(CommandKeyword.WITHPAYLOADS);
        }
        if (withSortKeys) {
            args.add(CommandKeyword.WITHSORTKEYS);
        }
        for (NumericFilter<K> filter : filters) {
            args.add(CommandKeyword.FILTER);
            filter.build(args);
        }
        if (geoFilter != null) {
            args.add(CommandKeyword.GEOFILTER);
            geoFilter.build(args);
        }
        if (!inKeys.isEmpty()) {
            args.add(CommandKeyword.INKEYS);
            args.add(inKeys.size());
            inKeys.forEach(args::addKey);
        }
        if (!inFields.isEmpty()) {
            args.add(CommandKeyword.INFIELDS);
            args.add(inFields.size());
            inFields.forEach(args::addKey);
        }
        if (!returnFields.isEmpty()) {
            args.add(CommandKeyword.RETURN);
            args.add(returnFields.size());
            returnFields.forEach(args::addKey);
        }
        if (summarize != null) {
            args.add(CommandKeyword.SUMMARIZE);
            summarize.build(args);
        }
        if (highlight != null) {
            args.add(CommandKeyword.HIGHLIGHT);
            highlight.build(args);
        }
        if (slop != null) {
            args.add(CommandKeyword.SLOP);
            args.add(slop);
        }
        if (inOrder) {
            args.add(CommandKeyword.INORDER);
        }
        if (language != null) {
            args.add(CommandKeyword.LANGUAGE);
            args.add(language.name());
        }
        if (expander != null) {
            args.add(CommandKeyword.EXPANDER);
            args.add(expander);
        }
        if (scorer != null) {
            args.add(CommandKeyword.SCORER);
            args.add(scorer);
        }
        if (payload != null) {
            args.add(CommandKeyword.PAYLOAD);
            args.addValue(payload);
        }
        if (sortBy != null) {
            args.add(CommandKeyword.SORTBY);
            sortBy.build(args);
        }
        if (limit != null) {
            limit.build(args);
        }
    }

    @SuppressWarnings("rawtypes")
    public static class Limit implements RediSearchArgument {

        private final long offset;
        private final long num;

        public Limit(long offset, long num) {
            this.offset = offset;
            this.num = num;
        }

        @Override
        public void build(RediSearchCommandArgs args) {
            args.add(CommandKeyword.LIMIT);
            args.add(offset);
            args.add(num);
        }

        public static LimitBuilder offset(long offset) {
            return new LimitBuilder(offset);
        }

        public static class LimitBuilder {

            private final long offset;

            public LimitBuilder(long offset) {
                this.offset = offset;
            }

            public Limit num(long num) {
                return new Limit(offset, num);
            }
        }

    }

    @SuppressWarnings("rawtypes")
    public static class NumericFilter<K> implements RediSearchArgument {

        private final K field;
        private final double min;
        private final double max;

        public NumericFilter(K field, double min, double max) {
            this.field = field;
            this.min = min;
            this.max = max;
        }

        @Override
        public void build(RediSearchCommandArgs args) {
            args.addKey(field);
            args.add(min);
            args.add(max);
        }

        public static <K> NumericFilterBuilder<K> field(K field) {
            return new NumericFilterBuilder<>(field);
        }

        public static class NumericFilterBuilder<K> {

            private final K field;

            public NumericFilterBuilder(K field) {
                this.field = field;
            }

            public MinNumericFilterBuilder<K> min(double min) {
                return new MinNumericFilterBuilder<>(field, min);
            }

        }

        public static class MinNumericFilterBuilder<K> {

            private final K field;
            private final double min;

            public MinNumericFilterBuilder(K field, double min) {
                this.field = field;
                this.min = min;
            }

            public NumericFilter<K> max(double max) {
                return new NumericFilter<>(field, min, max);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static class GeoFilter<K> implements RediSearchArgument {

        private final K field;
        private final double longitude;
        private final double latitude;
        private final double radius;
        private final String unit;

        public GeoFilter(K field, double longitude, double latitude, double radius, String unit) {
            this.field = field;
            this.longitude = longitude;
            this.latitude = latitude;
            this.radius = radius;
            this.unit = unit;
        }

        @Override
        public void build(RediSearchCommandArgs args) {
            args.addKey(field);
            args.add(longitude);
            args.add(latitude);
            args.add(radius);
            args.add(unit);
        }

        public static <K> GeoFilterBuilder<K> field(K field) {
            return new GeoFilterBuilder<>(field);
        }

        @Setter
        @Accessors(fluent = true)
        public static class GeoFilterBuilder<K> {

            private final K field;
            private double longitude;
            private double latitude;
            private double radius;
            private String unit;

            public GeoFilterBuilder(K field) {
                this.field = field;
            }

            public GeoFilter<K> build() {
                return new GeoFilter<>(field, longitude, latitude, radius, unit);
            }

        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Highlight<K, V> implements RediSearchArgument<K, V> {

        @Singular
        private List<K> fields;
        private SearchOptions.Tags<V> tags;

        @Override
        public void build(RediSearchCommandArgs<K, V> args) {
            if (fields.size() > 0) {
                args.add(CommandKeyword.FIELDS);
                args.add(fields.size());
                fields.forEach(args::addKey);
            }
            if (tags != null) {
                args.add(CommandKeyword.TAGS);
                args.addValue(tags.getOpen());
                args.addValue(tags.getClose());
            }
        }

    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Summarize<K, V> implements RediSearchArgument<K, V> {

        @Singular
        private List<K> fields;
        private Long frags;
        private Long length;
        private V separator;

        @Override
        public void build(RediSearchCommandArgs<K, V> args) {
            if (fields.size() > 0) {
                args.add(CommandKeyword.FIELDS);
                args.add(fields.size());
                fields.forEach(args::addKey);
            }
            if (frags != null) {
                args.add(CommandKeyword.FRAGS);
                args.add(frags);
            }
            if (length != null) {
                args.add(CommandKeyword.LEN);
                args.add(length);
            }
            if (separator != null) {
                args.add(CommandKeyword.SEPARATOR);
                args.addValue(separator);
            }
        }

    }

    @SuppressWarnings("rawtypes")
    public static class SortBy<K> implements RediSearchArgument {

        private final K field;
        private final Order direction;

        public SortBy(K field, Order direction) {
            this.field = field;
            this.direction = direction;
        }

        @Override
        public void build(RediSearchCommandArgs args) {
            args.addKey(field);
            args.add(direction == Order.ASC ? CommandKeyword.ASC : CommandKeyword.DESC);
        }

        public static <K> SortByBuilder<K> field(K field) {
            return new SortByBuilder<>(field);
        }

        public static class SortByBuilder<K> {

            private final K field;

            public SortByBuilder(K field) {
                this.field = field;
            }

            public SortBy<K> order(Order order) {
                return new SortBy<>(field, order);
            }
        }

    }

    @Data
    @Builder
    public static class Tags<V> {

        private V open;
        private V close;

    }
}
