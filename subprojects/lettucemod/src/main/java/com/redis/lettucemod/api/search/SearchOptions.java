package com.redis.lettucemod.api.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import lombok.Builder;
import lombok.Data;
import lombok.Setter;
import lombok.Singular;
import lombok.experimental.Accessors;

import java.util.List;

@Data
@Builder
public class SearchOptions<K, V> implements RediSearchArgument<K, V> {

    private boolean noContent;
    private boolean verbatim;
    private boolean noStopWords;
    private boolean withScores;
    private boolean withPayloads;
    private boolean withSortKeys;
    @Singular
    private List<NumericFilter<K, V>> filters;
    private GeoFilter<K, V> geoFilter;
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
    private SortBy<K, V> sortBy;
    private Limit limit;

    @Override
    public void build(SearchCommandArgs<K, V> args) {
        if (noContent) {
            args.add(SearchCommandKeyword.NOCONTENT);
        }
        if (verbatim) {
            args.add(SearchCommandKeyword.VERBATIM);
        }
        if (noStopWords) {
            args.add(SearchCommandKeyword.NOSTOPWORDS);
        }
        if (withScores) {
            args.add(SearchCommandKeyword.WITHSCORES);
        }
        if (withPayloads) {
            args.add(SearchCommandKeyword.WITHPAYLOADS);
        }
        if (withSortKeys) {
            args.add(SearchCommandKeyword.WITHSORTKEYS);
        }
        for (NumericFilter<K, V> filter : filters) {
            args.add(SearchCommandKeyword.FILTER);
            filter.build(args);
        }
        if (geoFilter != null) {
            args.add(SearchCommandKeyword.GEOFILTER);
            geoFilter.build(args);
        }
        if (!inKeys.isEmpty()) {
            args.add(SearchCommandKeyword.INKEYS);
            args.add(inKeys.size());
            inKeys.forEach(args::addKey);
        }
        if (!inFields.isEmpty()) {
            args.add(SearchCommandKeyword.INFIELDS);
            args.add(inFields.size());
            inFields.forEach(args::addKey);
        }
        if (!returnFields.isEmpty()) {
            args.add(SearchCommandKeyword.RETURN);
            args.add(returnFields.size());
            returnFields.forEach(args::addKey);
        }
        if (summarize != null) {
            args.add(SearchCommandKeyword.SUMMARIZE);
            summarize.build(args);
        }
        if (highlight != null) {
            args.add(SearchCommandKeyword.HIGHLIGHT);
            highlight.build(args);
        }
        if (slop != null) {
            args.add(SearchCommandKeyword.SLOP);
            args.add(slop);
        }
        if (inOrder) {
            args.add(SearchCommandKeyword.INORDER);
        }
        if (language != null) {
            args.add(SearchCommandKeyword.LANGUAGE);
            args.add(language.name());
        }
        if (expander != null) {
            args.add(SearchCommandKeyword.EXPANDER);
            args.add(expander);
        }
        if (scorer != null) {
            args.add(SearchCommandKeyword.SCORER);
            args.add(scorer);
        }
        if (payload != null) {
            args.add(SearchCommandKeyword.PAYLOAD);
            args.addValue(payload);
        }
        if (sortBy != null) {
            args.add(SearchCommandKeyword.SORTBY);
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
        public void build(SearchCommandArgs args) {
            args.add(SearchCommandKeyword.LIMIT);
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

    public static class NumericFilter<K, V> implements RediSearchArgument<K, V> {

        private final K field;
        private final double min;
        private final double max;

        public NumericFilter(K field, double min, double max) {
            this.field = field;
            this.min = min;
            this.max = max;
        }

        @Override
        public void build(SearchCommandArgs<K, V> args) {
            args.addKey(field);
            args.add(min);
            args.add(max);
        }

        public static <K, V> NumericFilterBuilder<K, V> field(K field) {
            return new NumericFilterBuilder<>(field);
        }

        public static class NumericFilterBuilder<K, V> {

            private final K field;

            public NumericFilterBuilder(K field) {
                this.field = field;
            }

            public MinNumericFilterBuilder<K, V> min(double min) {
                return new MinNumericFilterBuilder<>(field, min);
            }

        }

        public static class MinNumericFilterBuilder<K, V> {

            private final K field;
            private final double min;

            public MinNumericFilterBuilder(K field, double min) {
                this.field = field;
                this.min = min;
            }

            public NumericFilter<K, V> max(double max) {
                return new NumericFilter<>(field, min, max);
            }
        }
    }

    public static class GeoFilter<K, V> implements RediSearchArgument<K, V> {

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
        public void build(SearchCommandArgs<K, V> args) {
            args.addKey(field);
            args.add(longitude);
            args.add(latitude);
            args.add(radius);
            args.add(unit);
        }

        public static <K, V> GeoFilterBuilder<K, V> field(K field) {
            return new GeoFilterBuilder<>(field);
        }

        @Setter
        @Accessors(fluent = true)
        public static class GeoFilterBuilder<K, V> {

            private final K field;
            private double longitude;
            private double latitude;
            private double radius;
            private String unit;

            public GeoFilterBuilder(K field) {
                this.field = field;
            }

            public GeoFilter<K, V> build() {
                return new GeoFilter<>(field, longitude, latitude, radius, unit);
            }

        }
    }

    @Data
    @Builder
    public static class Highlight<K, V> implements RediSearchArgument<K, V> {

        @Singular
        private List<K> fields;
        private SearchOptions.Tags<V> tags;

        @Override
        public void build(SearchCommandArgs<K, V> args) {
            if (fields.size() > 0) {
                args.add(SearchCommandKeyword.FIELDS);
                args.add(fields.size());
                fields.forEach(args::addKey);
            }
            if (tags != null) {
                args.add(SearchCommandKeyword.TAGS);
                args.addValue(tags.getOpen());
                args.addValue(tags.getClose());
            }
        }

    }

    @Data
    @Builder
    public static class Summarize<K, V> implements RediSearchArgument<K, V> {

        @Singular
        private List<K> fields;
        private Long frags;
        private Long length;
        private V separator;

        @Override
        public void build(SearchCommandArgs<K, V> args) {
            if (fields.size() > 0) {
                args.add(SearchCommandKeyword.FIELDS);
                args.add(fields.size());
                fields.forEach(args::addKey);
            }
            if (frags != null) {
                args.add(SearchCommandKeyword.FRAGS);
                args.add(frags);
            }
            if (length != null) {
                args.add(SearchCommandKeyword.LEN);
                args.add(length);
            }
            if (separator != null) {
                args.add(SearchCommandKeyword.SEPARATOR);
                args.addValue(separator);
            }
        }

    }

    public static class SortBy<K, V> implements RediSearchArgument<K, V> {

        private final K field;
        private final Order direction;

        public SortBy(K field, Order direction) {
            this.field = field;
            this.direction = direction;
        }

        @Override
        public void build(SearchCommandArgs<K, V> args) {
            args.addKey(field);
            args.add(direction == Order.ASC ? SearchCommandKeyword.ASC : SearchCommandKeyword.DESC);
        }

        public static <K, V> SortByBuilder<K, V> field(K field) {
            return new SortByBuilder<>(field);
        }

        public static class SortByBuilder<K, V> {

            private final K field;

            public SortByBuilder(K field) {
                this.field = field;
            }

            public SortBy<K, V> order(Order order) {
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
