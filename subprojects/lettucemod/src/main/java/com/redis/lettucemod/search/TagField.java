package com.redis.lettucemod.search;

import java.util.Objects;
import java.util.Optional;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class TagField<K> extends Field<K> {

	private Optional<Character> separator = Optional.empty();
	private boolean caseSensitive;
	private boolean withSuffixTrie;

	private TagField(Builder<K> builder) {
		super(Type.TAG, builder);
		this.separator = builder.separator;
		this.caseSensitive = builder.caseSensitive;
		this.withSuffixTrie = builder.withSuffixTrie;
	}

	public Optional<Character> getSeparator() {
		return separator;
	}

	public void setSeparator(char separator) {
		this.separator = Optional.of(separator);
	}

	public boolean isCaseSensitive() {
		return caseSensitive;
	}

	public void setCaseSensitive(boolean caseSensitive) {
		this.caseSensitive = caseSensitive;
	}

	public boolean isWithSuffixTrie() {
		return withSuffixTrie;
	}

	public void setWithSuffixTrie(boolean withSuffixTrie) {
		this.withSuffixTrie = withSuffixTrie;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(caseSensitive, separator, withSuffixTrie);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		TagField<?> other = (TagField<?>) obj;
		return caseSensitive == other.caseSensitive && Objects.equals(separator, other.separator)
				&& withSuffixTrie == other.withSuffixTrie;
	}

	@Override
	protected void buildField(SearchCommandArgs<K, Object> args) {
		args.add(SearchCommandKeyword.TAG);
		separator.ifPresent(s -> args.add(SearchCommandKeyword.SEPARATOR).add(String.valueOf(s)));
		if (caseSensitive) {
			args.add(SearchCommandKeyword.CASESENSITIVE);
		}
		if (withSuffixTrie) {
			args.add(SearchCommandKeyword.WITHSUFFIXTRIE);
		}
	}

	public static <K> Builder<K> name(K name) {
		return new Builder<>(name);
	}

	public static class Builder<K> extends Field.Builder<K, Builder<K>> {

		private Optional<Character> separator = Optional.empty();
		private boolean caseSensitive;
		private boolean withSuffixTrie;

		public Builder(K name) {
			super(name);
		}

		public Builder<K> separator(char separator) {
			this.separator = Optional.of(separator);
			return this;
		}

		public Builder<K> caseSensitive() {
			this.caseSensitive = true;
			return this;
		}

		public Builder<K> withSuffixTrie() {
			this.withSuffixTrie = true;
			return this;
		}

		public TagField<K> build() {
			return new TagField<>(this);
		}

	}
}