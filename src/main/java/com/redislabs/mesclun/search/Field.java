package com.redislabs.mesclun.search;

import com.redislabs.mesclun.search.protocol.CommandKeyword;
import com.redislabs.mesclun.search.protocol.RediSearchCommandArgs;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
public abstract class Field<K, V> implements RediSearchArgument<K, V> {

    static final String MUST_NOT_BE_EMPTY = "must not be empty";
    static final String MUST_NOT_BE_NULL = "must not be null";

    private final Type type;
    private final K name;
    private boolean sortable;
    private boolean noIndex;

    protected Field(Type type, K name) {
        this.type = type;
        this.name = name;
    }

    @Override
    public void build(RediSearchCommandArgs<K, V> args) {
        args.addKey(name);
        buildField(args);
        if (sortable) {
            args.add(CommandKeyword.SORTABLE);
        }
        if (noIndex) {
            args.add(CommandKeyword.NOINDEX);
        }
    }

    @SuppressWarnings("rawtypes")
    protected abstract void buildField(RediSearchCommandArgs args);

    @SuppressWarnings("unchecked")
    protected static abstract class FieldBuilder<K, V, F extends Field<K, V>, B extends FieldBuilder<K, V, F, B>> {

        private final K name;
        private boolean sortable;
        private boolean noIndex;

        protected FieldBuilder(K name) {
            this.name = name;
        }

        public B sortable(boolean sortable) {
            this.sortable = sortable;
            return (B) this;
        }

        public B noIndex(boolean noIndex) {
            this.noIndex = noIndex;
            return (B) this;
        }

        public F build() {
            F field = newField(name);
            field.setNoIndex(noIndex);
            field.setSortable(sortable);
            return field;
        }

        protected abstract F newField(K name);

    }

    public static Text.TextFieldBuilder<String, String> text(String name) {
        return Text.builder(name);
    }

    public static Geo.GeoFieldBuilder<String, String> geo(String name) {
        return Geo.builder(name);
    }

    public static Tag.TagFieldBuilder<String, String> tag(String name) {
        return Tag.builder(name);
    }

    public static Numeric.NumericFieldBuilder<String, String> numeric(String name) {
        return Numeric.builder(name);
    }

    public static class Geo<K, V> extends Field<K, V> {

        public Geo(K name) {
            super(Type.GEO, name);
        }

        @SuppressWarnings("rawtypes")
        @Override
        protected void buildField(RediSearchCommandArgs args) {
            args.add(CommandKeyword.GEO);
        }

        public static <K, V> GeoFieldBuilder<K, V> builder(K name) {
            return new GeoFieldBuilder<>(name);
        }

        public static class GeoFieldBuilder<K, V> extends FieldBuilder<K, V, Geo<K, V>, GeoFieldBuilder<K, V>> {

            public GeoFieldBuilder(K name) {
                super(name);
            }

            @Override
            protected Geo<K, V> newField(K name) {
                return new Geo<>(name);
            }
        }
    }

    public static class Numeric<K, V> extends Field<K, V> {

        public Numeric(K name) {
            super(Type.NUMERIC, name);
        }

        @SuppressWarnings("rawtypes")
        @Override
        protected void buildField(RediSearchCommandArgs args) {
            args.add(CommandKeyword.NUMERIC);
        }

        public static <K, V> NumericFieldBuilder<K, V> builder(K name) {
            return new NumericFieldBuilder<>(name);
        }

        public static class NumericFieldBuilder<K, V> extends FieldBuilder<K, V, Numeric<K, V>, NumericFieldBuilder<K, V>> {

            public NumericFieldBuilder(K name) {
                super(name);
            }

            @Override
            protected Numeric<K, V> newField(K name) {
                return new Numeric<>(name);
            }

        }
    }

    @Getter
    @Setter
    public static class Tag<K, V> extends Field<K, V> {

        private String separator;

        public Tag(K name) {
            super(Type.TAG, name);
        }

        @SuppressWarnings("rawtypes")
        @Override
        protected void buildField(RediSearchCommandArgs args) {
            args.add(CommandKeyword.TAG);
            if (separator != null) {
                args.add(CommandKeyword.SEPARATOR);
                args.add(separator);
            }
        }

        public static <K, V> TagFieldBuilder<K, V> builder(K name) {
            return new TagFieldBuilder<>(name);
        }

        @Setter
        @Accessors(fluent = true)
        public static class TagFieldBuilder<K, V> extends FieldBuilder<K, V, Tag<K, V>, TagFieldBuilder<K, V>> {

            private String separator;

            public TagFieldBuilder(K name) {
                super(name);
            }

            @Override
            protected Tag<K, V> newField(K name) {
                Tag<K, V> field = new Tag<>(name);
                field.setSeparator(separator);
                return field;
            }
        }
    }

    @Getter
    @Setter
    public static class Text<K, V> extends Field<K, V> {

        private Double weight;
        private boolean noStem;
        private PhoneticMatcher matcher;

        public Text(K name) {
            super(Type.TEXT, name);
        }

        @SuppressWarnings("rawtypes")
        @Override
        protected void buildField(RediSearchCommandArgs args) {
            args.add(CommandKeyword.TEXT);
            if (noStem) {
                args.add(CommandKeyword.NOSTEM);
            }
            if (weight != null) {
                args.add(CommandKeyword.WEIGHT);
                args.add(weight);
            }
            if (matcher != null) {
                args.add(CommandKeyword.PHONETIC);
                args.add(matcher.getCode());
            }
        }

        public static <K, V> TextFieldBuilder<K, V> builder(K name) {
            return new TextFieldBuilder<>(name);
        }

        @Setter
        @Accessors(fluent = true)
        public static class TextFieldBuilder<K, V> extends FieldBuilder<K, V, Text<K, V>, TextFieldBuilder<K, V>> {

            private Double weight;
            private boolean noStem;
            private PhoneticMatcher matcher;

            public TextFieldBuilder(K name) {
                super(name);
            }

            @Override
            protected Text<K, V> newField(K name) {
                Text<K, V> field = new Text<>(name);
                field.setWeight(weight);
                field.setNoStem(noStem);
                field.setMatcher(matcher);
                return field;
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
}