package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.search.Field.GeoField.GeoFieldBuilder;
import com.redis.lettucemod.search.Field.NumericField.NumericFieldBuilder;
import com.redis.lettucemod.search.Field.TagField.TagFieldBuilder;
import com.redis.lettucemod.search.Field.TextField.TextFieldBuilder;

import io.lettuce.core.internal.LettuceAssert;

@SuppressWarnings("rawtypes")
public abstract class Field implements RediSearchArgument {

	private final Type type;
	private final String name;
	private String as;
	private boolean sortable;
	private boolean unNormalizedForm;
	private boolean noIndex;

	protected Field(Type type, String name) {
		LettuceAssert.notNull(type, "A type is required");
		LettuceAssert.notNull(name, "A name is required");
		this.type = type;
		this.name = name;
	}

	public Type getType() {
		return type;
	}

	public String getName() {
		return name;
	}

	public String getAs() {
		return as;
	}

	public void setAs(String as) {
		this.as = as;
	}

	public boolean isSortable() {
		return sortable;
	}

	public void setSortable(boolean sortable) {
		this.sortable = sortable;
	}

	public boolean isUnNormalizedForm() {
		return unNormalizedForm;
	}

	public void setUnNormalizedForm(boolean unNormalizedForm) {
		this.unNormalizedForm = unNormalizedForm;
	}

	public boolean isNoIndex() {
		return noIndex;
	}

	public void setNoIndex(boolean noIndex) {
		this.noIndex = noIndex;
	}

	@Override
	public void build(SearchCommandArgs args) {
		args.add(name);
		if (as != null) {
			args.add(SearchCommandKeyword.AS);
			args.add(as);
		}
		buildField(args);
		if (sortable) {
			args.add(SearchCommandKeyword.SORTABLE);
			if (unNormalizedForm) {
				args.add(SearchCommandKeyword.UNF);
			}
		}
		if (noIndex) {
			args.add(SearchCommandKeyword.NOINDEX);
		}
	}

	protected abstract void buildField(SearchCommandArgs args);

	@SuppressWarnings("unchecked")
	protected abstract static class FieldBuilder<F extends Field, B extends FieldBuilder<F, B>> {

		protected final String name;
		private String as;
		private boolean sortable;
		private boolean unNormalizedForm;
		private boolean noIndex;

		protected FieldBuilder(String name) {
			this.name = name;
		}

		public B as(String as) {
			this.as = as;
			return (B) this;
		}

		public B sortable() {
			this.sortable = true;
			return (B) this;
		}

		public B unNormalizedForm() {
			this.sortable = true;
			this.unNormalizedForm = true;
			return (B) this;
		}

		public B noIndex() {
			this.noIndex = true;
			return (B) this;
		}

		public abstract F newField();

		public F build() {
			F field = newField();
			field.setAs(as);
			field.setSortable(sortable);
			field.setUnNormalizedForm(unNormalizedForm);
			field.setNoIndex(noIndex);
			return field;
		}

	}

	public static TextFieldBuilder text(String name) {
		return TextField.builder(name);
	}

	public static GeoFieldBuilder geo(String name) {
		return GeoField.builder(name);
	}

	public static TagFieldBuilder tag(String name) {
		return TagField.builder(name);
	}

	public static NumericFieldBuilder numeric(String name) {
		return NumericField.builder(name);
	}

	public static class GeoField extends Field {

		public GeoField(String name) {
			super(Type.GEO, name);
		}

		@Override
		protected void buildField(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.GEO);
		}

		public static GeoFieldBuilder builder(String name) {
			return new GeoFieldBuilder(name);
		}

		public static class GeoFieldBuilder extends FieldBuilder<GeoField, GeoFieldBuilder> {

			public GeoFieldBuilder(String name) {
				super(name);
			}

			@Override
			public GeoField newField() {
				return new GeoField(name);
			}

		}
	}

	public static class NumericField extends Field {

		public NumericField(String name) {
			super(Type.NUMERIC, name);
		}

		@Override
		protected void buildField(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.NUMERIC);
		}

		public static NumericFieldBuilder builder(String name) {
			return new NumericFieldBuilder(name);
		}

		public static class NumericFieldBuilder extends FieldBuilder<NumericField, NumericFieldBuilder> {

			public NumericFieldBuilder(String name) {
				super(name);
			}

			@Override
			public NumericField newField() {
				return new NumericField(name);
			}

		}
	}

	public static class TagField extends Field {

		private String separator;
		private boolean caseSensitive;

		public TagField(String name) {
			super(Type.TAG, name);
		}

		public String getSeparator() {
			return separator;
		}

		public void setSeparator(String separator) {
			this.separator = separator;
		}

		public boolean isCaseSensitive() {
			return caseSensitive;
		}

		public void setCaseSensitive(boolean caseSensitive) {
			this.caseSensitive = caseSensitive;
		}

		@Override
		protected void buildField(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.TAG);
			if (separator != null) {
				args.add(SearchCommandKeyword.SEPARATOR);
				args.add(separator);
			}
			if (caseSensitive) {
				args.add(SearchCommandKeyword.CASESENSITIVE);
			}

		}

		public static TagFieldBuilder builder(String name) {
			return new TagFieldBuilder(name);
		}

		public static class TagFieldBuilder extends FieldBuilder<TagField, TagFieldBuilder> {

			private String separator;
			private boolean caseSensitive;

			public TagFieldBuilder(String name) {
				super(name);
			}

			public TagFieldBuilder separator(String separator) {
				this.separator = separator;
				return this;
			}

			public TagFieldBuilder caseSensitive() {
				this.caseSensitive = true;
				return this;
			}

			@Override
			public TagField newField() {
				TagField field = new TagField(name);
				field.setSeparator(separator);
				field.setCaseSensitive(caseSensitive);
				return field;
			}

		}
	}

	public static class TextField extends Field {

		private Double weight;
		private boolean noStem;
		private PhoneticMatcher matcher;

		public TextField(String name) {
			super(Type.TEXT, name);
		}

		public Double getWeight() {
			return weight;
		}

		public void setWeight(Double weight) {
			this.weight = weight;
		}

		public boolean isNoStem() {
			return noStem;
		}

		public void setNoStem(boolean noStem) {
			this.noStem = noStem;
		}

		public PhoneticMatcher getMatcher() {
			return matcher;
		}

		public void setMatcher(PhoneticMatcher matcher) {
			this.matcher = matcher;
		}

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

		public static class TextFieldBuilder extends FieldBuilder<TextField, TextFieldBuilder> {

			private boolean noStem;
			private Double weight;
			private PhoneticMatcher matcher;

			public TextFieldBuilder(String name) {
				super(name);
			}

			public TextFieldBuilder noStem() {
				this.noStem = true;
				return this;
			}

			public TextFieldBuilder weight(double weight) {
				this.weight = weight;
				return this;
			}

			public TextFieldBuilder matcher(PhoneticMatcher matcher) {
				this.matcher = matcher;
				return this;
			}

			@Override
			public TextField newField() {
				TextField field = new TextField(name);
				field.setNoStem(noStem);
				field.setWeight(weight);
				field.setMatcher(matcher);
				return field;
			}

		}

		public enum PhoneticMatcher {

			ENGLISH("dm:en"), FRENCH("dm:fr"), PORTUGUESE("dm:pt"), SPANISH("dm:es");

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
