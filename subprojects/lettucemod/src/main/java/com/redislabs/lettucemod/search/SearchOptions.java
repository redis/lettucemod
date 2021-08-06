package com.redislabs.lettucemod.search;

import com.redislabs.lettucemod.search.protocol.CommandKeyword;
import com.redislabs.lettucemod.search.protocol.RediSearchCommandArgs;
import lombok.*;
import lombok.experimental.Accessors;

import java.util.List;

@SuppressWarnings("rawtypes")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SearchOptions implements RediSearchArgument {

    private boolean noContent;
    private boolean verbatim;
    private boolean noStopWords;
    private boolean withScores;
    private boolean withPayloads;
    private boolean withSortKeys;
    @Singular
    private List<NumericFilter> filters;
    private GeoFilter geoFilter;
    @Singular
    private List<String> inKeys;
    @Singular
    private List<String> inFields;
    @Singular
    private List<String> returnFields;
    private Summarize summarize;
    private Highlight highlight;
    private Long slop;
    private boolean inOrder;
    private Language language;
    private String expander;
    private String scorer;
    private String payload;
    private SortBy sortBy;
    private Limit limit;

    @Override
    public void build(RediSearchCommandArgs args) {
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
        for (NumericFilter filter : filters) {
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
            inKeys.forEach(args::add);
        }
        if (!inFields.isEmpty()) {
            args.add(CommandKeyword.INFIELDS);
            args.add(inFields.size());
            inFields.forEach(args::add);
        }
        if (!returnFields.isEmpty()) {
            args.add(CommandKeyword.RETURN);
            args.add(returnFields.size());
            returnFields.forEach(args::add);
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
            args.add(payload);
        }
        if (sortBy != null) {
            args.add(CommandKeyword.SORTBY);
            sortBy.build(args);
        }
        if (limit != null) {
            limit.build(args);
        }
    }

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

    public static class NumericFilter implements RediSearchArgument {

        private final String field;
        private final double min;
        private final double max;

        public NumericFilter(String field, double min, double max) {
            this.field = field;
            this.min = min;
            this.max = max;
        }

        @Override
        public void build(RediSearchCommandArgs args) {
            args.add(field);
            args.add(min);
            args.add(max);
        }

        public static NumericFilterBuilder field(String field) {
            return new NumericFilterBuilder(field);
        }

        public static class NumericFilterBuilder {

            private final String field;

            public NumericFilterBuilder(String field) {
                this.field = field;
            }

            public MinNumericFilterBuilder min(double min) {
                return new MinNumericFilterBuilder(field, min);
            }

        }

        public static class MinNumericFilterBuilder {

            private final String field;
            private final double min;

            public MinNumericFilterBuilder(String field, double min) {
                this.field = field;
                this.min = min;
            }

            public NumericFilter max(double max) {
                return new NumericFilter(field, min, max);
            }
        }
    }

    public static class GeoFilter implements RediSearchArgument {

        private final String field;
        private final double longitude;
        private final double latitude;
        private final double radius;
        private final String unit;

        public GeoFilter(String field, double longitude, double latitude, double radius, String unit) {
            this.field = field;
            this.longitude = longitude;
            this.latitude = latitude;
            this.radius = radius;
            this.unit = unit;
        }

        @Override
        public void build(RediSearchCommandArgs args) {
            args.add(field);
            args.add(longitude);
            args.add(latitude);
            args.add(radius);
            args.add(unit);
        }

        public static GeoFilterBuilder field(String field) {
            return new GeoFilterBuilder(field);
        }

        @Setter
        @Accessors(fluent = true)
        public static class GeoFilterBuilder {

            private final String field;
            private double longitude;
            private double latitude;
            private double radius;
            private String unit;

            public GeoFilterBuilder(String field) {
                this.field = field;
            }

            public GeoFilter build() {
                return new GeoFilter(field, longitude, latitude, radius, unit);
            }

        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Highlight implements RediSearchArgument {

        @Singular
        private List<String> fields;
        private SearchOptions.Tags tags;

        @Override
        public void build(RediSearchCommandArgs args) {
            if (fields.size() > 0) {
                args.add(CommandKeyword.FIELDS);
                args.add(fields.size());
                fields.forEach(args::add);
            }
            if (tags != null) {
                args.add(CommandKeyword.TAGS);
                args.add(tags.getOpen());
                args.add(tags.getClose());
            }
        }

    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Summarize implements RediSearchArgument {

        @Singular
        private List<String> fields;
        private Long frags;
        private Long length;
        private String separator;

        @Override
        public void build(RediSearchCommandArgs args) {
            if (fields.size() > 0) {
                args.add(CommandKeyword.FIELDS);
                args.add(fields.size());
                fields.forEach(args::add);
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
                args.add(separator);
            }
        }

    }

    public static class SortBy implements RediSearchArgument {

        private final String field;
        private final Order direction;

        public SortBy(String field, Order direction) {
            this.field = field;
            this.direction = direction;
        }

        @Override
        public void build(RediSearchCommandArgs args) {
            args.add(field);
            args.add(direction == Order.ASC ? CommandKeyword.ASC : CommandKeyword.DESC);
        }

        public static SortByBuilder field(String field) {
            return new SortByBuilder(field);
        }

        public static class SortByBuilder {

            private final String field;

            public SortByBuilder(String field) {
                this.field = field;
            }

            public SortBy order(Order order) {
                return new SortBy(field, order);
            }
        }

    }

    @Data
    @Builder
    public static class Tags {

        private String open;
        private String close;

    }
}
