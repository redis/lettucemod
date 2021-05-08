package com.redislabs.mesclun.search;

import com.redislabs.mesclun.search.protocol.CommandKeyword;
import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;
import lombok.*;

import java.util.List;

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
    private Highlight<K, V> highlight;
    private Language language;
    private String expander;
    private String scorer;
    private V payload;
    private SortBy<K> sortBy;
    private Limit limit;

    @SuppressWarnings("unchecked")
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
            filter.build((RediSearchCommandArgs<K, Object>) args);
        }
        if (geoFilter != null) {
            args.add(CommandKeyword.GEOFILTER);
            geoFilter.build((RediSearchCommandArgs<K, Object>) args);
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
        if (highlight != null) {
            args.add(CommandKeyword.HIGHLIGHT);
            highlight.build(args);
        }
        if (sortBy != null) {
            args.add(CommandKeyword.SORTBY);
            sortBy.build((RediSearchCommandArgs<K, Object>) args);
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
        if (limit != null) {
            limit.build((RediSearchCommandArgs<Object, Object>) args);
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NumericFilter<K> implements RediSearchArgument<K, Object> {

        private K field;
        private double min;
        private double max;

        @Override
        public void build(RediSearchCommandArgs<K, Object> args) {
            args.addKey(field);
            args.add(min);
            args.add(max);
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GeoFilter<K> implements RediSearchArgument<K, Object> {

        private K field;
        private double longitude;
        private double latitude;
        private double radius;
        private String unit;

        @Override
        public void build(RediSearchCommandArgs<K, Object> args) {
            args.addKey(field);
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
    public static class Highlight<K, V> implements RediSearchArgument<K, V> {

        @Singular
        private List<K> fields;
        private Tag<V> tag;

        @Override
        public void build(RediSearchCommandArgs<K, V> args) {
            if (fields.size() > 0) {
                args.add(CommandKeyword.FIELDS);
                args.add(fields.size());
                fields.forEach(args::addKey);
            }
            if (tag != null) {
                args.add(CommandKeyword.TAGS);
                args.addValue(tag.getOpen());
                args.addValue(tag.getClose());
            }
        }

        @Data
        @Builder
        public static class Tag<V> {

            private V open;
            private V close;

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
    public static class SortBy<K> implements RediSearchArgument<K, Object> {

        public final static Direction DEFAULT_DIRECTION = Direction.Ascending;

        private K field;
        @Builder.Default
        private Direction direction = DEFAULT_DIRECTION;

        @Override
        public void build(RediSearchCommandArgs<K, Object> args) {
            args.addKey(field);
            args.add(direction == Direction.Ascending ? CommandKeyword.ASC : CommandKeyword.DESC);
        }

        @SuppressWarnings("unused")
        public enum Direction {
            Ascending, Descending
        }
    }
}
