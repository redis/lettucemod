package com.redislabs.mesclun.search;

import com.redislabs.mesclun.search.protocol.CommandKeyword;
import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;
import lombok.*;

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
    private Highlight highlight;
    private Language language;
    private String expander;
    private String scorer;
    private String payload;
    private SortBy sortBy;
    private Limit limit;

    @SuppressWarnings("unchecked")
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
        if (highlight != null) {
            args.add(CommandKeyword.HIGHLIGHT);
            highlight.build(args);
        }
        if (sortBy != null) {
            args.add(CommandKeyword.SORTBY);
            sortBy.build(args);
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
        if (limit != null) {
            limit.build(args);
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NumericFilter implements RediSearchArgument {

        private String field;
        private double min;
        private double max;

        @Override
        public void build(RediSearchCommandArgs args) {
            args.add(field);
            args.add(min);
            args.add(max);
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GeoFilter implements RediSearchArgument {

        private String field;
        private double longitude;
        private double latitude;
        private double radius;
        private String unit;

        @Override
        public void build(RediSearchCommandArgs args) {
            args.add(field);
            args.add(longitude);
            args.add(latitude);
            args.add(radius);
            args.add(unit);
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Highlight implements RediSearchArgument {

        @Singular
        private List<String> fields;
        private Tag tag;

        @Override
        public void build(RediSearchCommandArgs args) {
            if (fields.size() > 0) {
                args.add(CommandKeyword.FIELDS);
                args.add(fields.size());
                fields.forEach(args::add);
            }
            if (tag != null) {
                args.add(CommandKeyword.TAGS);
                args.add(tag.getOpen());
                args.add(tag.getClose());
            }
        }

        @Data
        @Builder
        public static class Tag {

            private String open;
            private String close;

        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Limit implements RediSearchArgument<Object, Object> {

        private long offset;
        private  long num;

        @Override
        public void build(RediSearchCommandArgs<Object, Object> args) {
            args.add(CommandKeyword.LIMIT);
            args.add(offset);
            args.add(num);
        }

    }

    @SuppressWarnings("unused")
    public enum Language {
        Arabic, Danish, Dutch, English, Finnish, French, German, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian, Spanish, Swedish, Tamil, Turkish, Chinese
    }

    @Data
    @Builder
    public static class SortBy implements RediSearchArgument {

        public final static Direction DEFAULT_DIRECTION = Direction.Ascending;

        private String field;
        @Builder.Default
        private Direction direction = DEFAULT_DIRECTION;

        @Override
        public void build(RediSearchCommandArgs args) {
            args.add(field);
            args.add(direction == Direction.Ascending ? CommandKeyword.ASC : CommandKeyword.DESC);
        }

        @SuppressWarnings("unused")
        public enum Direction {
            Ascending, Descending
        }
    }
}
