package com.redis.lettucemod.api.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import io.lettuce.core.internal.LettuceAssert;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings("rawtypes")
@Getter
public abstract class Field implements RediSearchArgument {

    private final Type type;
    private final String name;
    private final Options options;

    protected Field(Type type, String name, Options options) {
        LettuceAssert.notNull(type, "A type is required");
        LettuceAssert.notNull(name, "A name is required");
        LettuceAssert.notNull(options, "Options are required");
        this.type = type;
        this.name = name;
        this.options = options;
    }

    @Override
    public void build(SearchCommandArgs args) {
        args.add(name);
        if (options.getAs() != null) {
            args.add(SearchCommandKeyword.AS);
            args.add(options.getAs());
        }
        buildField(args);
        if (options.isCaseSensitive()) {
            args.add(SearchCommandKeyword.CASESENSITIVE);
        }
        if (options.isSortable()) {
            args.add(SearchCommandKeyword.SORTABLE);
            if (options.isUnNormalizedForm()) {
                args.add(SearchCommandKeyword.UNF);
            }
        }
        if (options.isNoIndex()) {
            args.add(SearchCommandKeyword.NOINDEX);
        }
    }

    @SuppressWarnings("rawtypes")
    protected abstract void buildField(SearchCommandArgs args);

    @SuppressWarnings("unchecked")
    protected static abstract class FieldBuilder<F extends Field, B extends FieldBuilder<F, B>> {

        protected final String name;
        protected Options options = Options.builder().build();

        protected FieldBuilder(String name) {
            this.name = name;
        }

        public B options(Options options) {
            LettuceAssert.notNull(options, "Options must not be null");
            this.options = options;
            return (B) this;
        }

        public B sortable() {
            options.setSortable(true);
            return (B) this;
        }

        public B sortableUNF() {
            options.setSortable(true);
            options.setUnNormalizedForm(true);
            return (B) this;
        }

        public B noIndex() {
            options.setNoIndex(true);
            return (B) this;
        }

        public B caseSensitive() {
            options.setCaseSensitive(true);
            return (B) this;
        }

        public B as(String attribute) {
            options.setAs(attribute);
            return (B) this;
        }

        public abstract F build();

    }

    public static Text.TextFieldBuilder text(String name) {
        return Text.builder(name);
    }

    public static Geo.GeoFieldBuilder geo(String name) {
        return Geo.builder(name);
    }

    public static Tag.TagFieldBuilder tag(String name) {
        return Tag.builder(name);
    }

    public static Numeric.NumericFieldBuilder numeric(String name) {
        return Numeric.builder(name);
    }

    public static class Geo extends Field {

        public Geo(String name, Options options) {
            super(Type.GEO, name, options);
        }

        @SuppressWarnings("rawtypes")
        @Override
        protected void buildField(SearchCommandArgs args) {
            args.add(SearchCommandKeyword.GEO);
        }

        public static GeoFieldBuilder builder(String name) {
            return new GeoFieldBuilder(name);
        }

        public static class GeoFieldBuilder extends FieldBuilder<Geo, GeoFieldBuilder> {

            public GeoFieldBuilder(String name) {
                super(name);
            }

            @Override
            public Geo build() {
                return new Geo(name, options);
            }
        }
    }

    public static class Numeric extends Field {

        public Numeric(String name, Options options) {
            super(Type.NUMERIC, name, options);
        }

        @SuppressWarnings("rawtypes")
        @Override
        protected void buildField(SearchCommandArgs args) {
            args.add(SearchCommandKeyword.NUMERIC);
        }

        public static NumericFieldBuilder builder(String name) {
            return new NumericFieldBuilder(name);
        }

        public static class NumericFieldBuilder extends FieldBuilder<Numeric, NumericFieldBuilder> {

            public NumericFieldBuilder(String name) {
                super(name);
            }

            @Override
            public Numeric build() {
                return new Numeric(name, options);
            }

        }
    }

    @Getter
    public static class Tag extends Field {

        private final String separator;

        public Tag(String name, Options options, String separator) {
            super(Type.TAG, name, options);
            this.separator = separator;
        }

        @SuppressWarnings("rawtypes")
        @Override
        protected void buildField(SearchCommandArgs args) {
            args.add(SearchCommandKeyword.TAG);
            if (separator != null) {
                args.add(SearchCommandKeyword.SEPARATOR);
                args.add(separator);
            }
        }

        public static TagFieldBuilder builder(String name) {
            return new TagFieldBuilder(name);
        }

        @Setter
        @Accessors(fluent = true)
        public static class TagFieldBuilder extends FieldBuilder<Tag, TagFieldBuilder> {

            private String separator;

            public TagFieldBuilder(String name) {
                super(name);
            }

            @Override
            public Tag build() {
                return new Tag(name, options, separator);
            }

        }
    }

    @Getter
    public static class Text extends Field {

        private final Double weight;
        private final boolean noStem;
        private final PhoneticMatcher matcher;

        public Text(String name, Options options, Double weight, boolean noStem) {
            this(name, options, weight, noStem, null);
        }

        public Text(String name, Options options, Double weight, boolean noStem, PhoneticMatcher matcher) {
            super(Type.TEXT, name, options);
            this.weight = weight;
            this.noStem = noStem;
            this.matcher = matcher;
        }

        @SuppressWarnings("rawtypes")
        @Override
        protected void buildField(SearchCommandArgs args) {
            args.add(SearchCommandKeyword.TEXT);
            if (noStem) {
                args.add(SearchCommandKeyword.NOSTEM);
            }
            if (weight != null) {
                args.add(SearchCommandKeyword.WEIGHT);
                args.add(weight);
            }
            if (matcher != null) {
                args.add(SearchCommandKeyword.PHONETIC);
                args.add(matcher.getCode());
            }
        }

        public static TextFieldBuilder builder(String name) {
            return new TextFieldBuilder(name);
        }

        @Setter
        @Accessors(fluent = true)
        public static class TextFieldBuilder extends FieldBuilder<Text, TextFieldBuilder> {

            private Double weight;
            private boolean noStem;
            private PhoneticMatcher matcher;

            public TextFieldBuilder(String name) {
                super(name);
            }

            @Override
            public Text build() {
                return new Text(name, options, weight, noStem, matcher);
            }

        }

        public enum PhoneticMatcher {

            English("dm:en"), French("dm:fr"), Portuguese("dm:pt"), Spanish("dm:es");

            private final String code;

            PhoneticMatcher(String code) {
                this.code = code;
            }

            public String getCode() {
                return code;
            }
        }
    }

    public enum Type {
        TEXT, NUMERIC, GEO, TAG
    }

    @Data
    @Builder
    public static class Options {
        private String as;
        private boolean caseSensitive;
        private boolean sortable;
        private boolean unNormalizedForm;
        private boolean noIndex;

    }

}
