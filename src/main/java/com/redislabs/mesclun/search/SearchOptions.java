package com.redislabs.mesclun.search;

import com.redislabs.mesclun.search.protocol.CommandKeyword;
import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;
import lombok.*;

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
    private NumericFilter<K, V> filter;
    @Singular
    private List<K> inKeys;
    @Singular
    private List<K> inFields;
    @Singular
    private List<K> returnFields;
    private Highlight<K, V> highlight;
    private Language language;
    private SortBy<K, V> sortBy;
    private Limit limit;

    @SuppressWarnings({"unchecked", "rawtypes"})
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
            sortBy.build(args);
        }
        if (language != null) {
            args.add(CommandKeyword.LANGUAGE);
            args.add(language.name());
        }
        if (limit != null) {
            limit.build(args);
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NumericFilter<K, V> implements RediSearchArgument<K, V> {

        private K field;
        private double min;
        private double max;

        @Override
        public void build(RediSearchCommandArgs<K, V> args) {
            args.addKey(field);
            args.add(min);
            args.add(max);
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

    }

    @SuppressWarnings("unused")
    public enum Language {
        Arabic, Danish, Dutch, English, Finnish, French, German, Hungarian, Italian, Norwegian, Portuguese, Romanian, Russian, Spanish, Swedish, Tamil, Turkish, Chinese
    }

    @Data
    @Builder
    public static class SortBy<K, V> implements RediSearchArgument<K, V> {

        public final static Direction DEFAULT_DIRECTION = Direction.Ascending;

        private K field;
        @Builder.Default
        private Direction direction = DEFAULT_DIRECTION;

        @Override
        public void build(RediSearchCommandArgs<K, V> args) {
            args.addKey(field);
            args.add(direction == Direction.Ascending ? CommandKeyword.ASC : CommandKeyword.DESC);
        }

        @SuppressWarnings("unused")
        public enum Direction {
            Ascending, Descending
        }
    }
}
