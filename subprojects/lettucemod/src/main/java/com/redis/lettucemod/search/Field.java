package com.redis.lettucemod.search;

import java.util.Optional;
import java.util.OptionalDouble;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.search.Field.GeoField.Builder;

import io.lettuce.core.internal.LettuceAssert;

@SuppressWarnings("rawtypes")
public abstract class Field implements RediSearchArgument {

	private final Type type;
	private final String name;
	protected Optional<As> as = Optional.empty();
	protected boolean sortable;
	protected boolean unNormalizedForm;
	protected boolean noIndex;

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

	public Optional<String> getAs() {
		return as.map(As::getField);
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
		as.ifPresent(a -> a.build(args));
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
	protected abstract static class AbstractBuilder<F extends Field, B extends AbstractBuilder<F, B>> {

		protected final String name;
		private Optional<As> as = Optional.empty();
		private boolean sortable;
		private boolean unNormalizedForm;
		private boolean noIndex;

		protected AbstractBuilder(String name) {
			this.name = name;
		}

		public B as(String as) {
			this.as = Optional.of(new As(as));
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

		protected abstract F newField();

		public F build() {
			F field = newField();
			field.as = as;
			field.sortable = sortable;
			field.unNormalizedForm = unNormalizedForm;
			field.noIndex = noIndex;
			return field;
		}

	}

	public static TextField.Builder text(String name) {
		return TextField.builder(name);
	}

	public static Builder geo(String name) {
		return GeoField.builder(name);
	}

	public static TagField.Builder tag(String name) {
		return TagField.builder(name);
	}

	public static NumericField.Builder numeric(String name) {
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

		public static Builder builder(String name) {
			return new Builder(name);
		}

		public static class Builder extends AbstractBuilder<GeoField, Builder> {

			public Builder(String name) {
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

		public static Builder builder(String name) {
			return new Builder(name);
		}

		public static class Builder extends AbstractBuilder<NumericField, Builder> {

			public Builder(String name) {
				super(name);
			}

			@Override
			public NumericField newField() {
				return new NumericField(name);
			}

		}
	}

	public static class TagField extends Field {

		private Optional<String> separator = Optional.empty();
		private boolean caseSensitive;

		public TagField(String name) {
			super(Type.TAG, name);
		}

		public Optional<String> getSeparator() {
			return separator;
		}

		public void setSeparator(String separator) {
			this.separator = Optional.of(separator);
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
			separator.ifPresent(s -> args.add(SearchCommandKeyword.SEPARATOR).add(s));
			if (caseSensitive) {
				args.add(SearchCommandKeyword.CASESENSITIVE);
			}

		}

		public static Builder builder(String name) {
			return new Builder(name);
		}

		public static class Builder extends AbstractBuilder<TagField, Builder> {

			private Optional<String> separator = Optional.empty();
			private boolean caseSensitive;

			public Builder(String name) {
				super(name);
			}

			public Builder separator(String separator) {
				this.separator = Optional.of(separator);
				return this;
			}

			public Builder caseSensitive() {
				this.caseSensitive = true;
				return this;
			}

			@Override
			public TagField newField() {
				TagField field = new TagField(name);
				separator.ifPresent(field::setSeparator);
				field.setCaseSensitive(caseSensitive);
				return field;
			}

		}
	}

	public static class TextField extends Field {

		private OptionalDouble weight = OptionalDouble.empty();
		private boolean noStem;
		private Optional<PhoneticMatcher> matcher = Optional.empty();

		public TextField(String name) {
			super(Type.TEXT, name);
		}

		public OptionalDouble getWeight() {
			return weight;
		}

		public void setWeight(Double weight) {
			this.weight = OptionalDouble.of(weight);
		}

		public boolean isNoStem() {
			return noStem;
		}

		public void setNoStem(boolean noStem) {
			this.noStem = noStem;
		}

		public Optional<PhoneticMatcher> getMatcher() {
			return matcher;
		}

		public void setMatcher(PhoneticMatcher matcher) {
			this.matcher = Optional.of(matcher);
		}

		@Override
		protected void buildField(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.TEXT);
			if (noStem) {
				args.add(SearchCommandKeyword.NOSTEM);
			}
			weight.ifPresent(w -> args.add(SearchCommandKeyword.WEIGHT).add(w));
			matcher.ifPresent(m -> args.add(SearchCommandKeyword.PHONETIC).add(m.getCode()));
		}

		public static Builder builder(String name) {
			return new Builder(name);
		}

		public static class Builder extends AbstractBuilder<TextField, Builder> {

			private boolean noStem;
			private OptionalDouble weight = OptionalDouble.empty();
			private Optional<PhoneticMatcher> matcher = Optional.empty();

			public Builder(String name) {
				super(name);
			}

			public Builder noStem() {
				this.noStem = true;
				return this;
			}

			public Builder weight(double weight) {
				this.weight = OptionalDouble.of(weight);
				return this;
			}

			public Builder matcher(PhoneticMatcher matcher) {
				this.matcher = Optional.of(matcher);
				return this;
			}

			@Override
			public TextField newField() {
				TextField field = new TextField(name);
				field.setNoStem(noStem);
				weight.ifPresent(field::setWeight);
				matcher.ifPresent(field::setMatcher);
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
