package com.redis.lettucemod.search;

import java.util.Optional;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class TagField<K> extends Field<K> {

	private Optional<String> separator = Optional.empty();
	private boolean caseSensitive;

	private TagField(Builder<K> builder) {
		super(Type.TAG, builder);
		this.separator = builder.separator;
		this.caseSensitive = builder.caseSensitive;
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
	protected void buildField(SearchCommandArgs<K, Object> args) {
		args.add(SearchCommandKeyword.TAG);
		separator.ifPresent(s -> args.add(SearchCommandKeyword.SEPARATOR).add(s));
		if (caseSensitive) {
			args.add(SearchCommandKeyword.CASESENSITIVE);
		}

	}

	public static <K> Builder<K> name(K name) {
		return new Builder<>(name);
	}

	public static class Builder<K> extends Field.Builder<K, Builder<K>> {

		private Optional<String> separator = Optional.empty();
		private boolean caseSensitive;

		public Builder(K name) {
			super(name);
		}

		public Builder<K> separator(String separator) {
			this.separator = Optional.of(separator);
			return this;
		}

		public Builder<K> caseSensitive() {
			this.caseSensitive = true;
			return this;
		}

		public TagField<K> build() {
			return new TagField<>(this);
		}

	}
}