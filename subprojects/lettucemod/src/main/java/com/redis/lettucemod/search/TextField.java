package com.redis.lettucemod.search;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class TextField<K> extends Field<K> {

	private OptionalDouble weight = OptionalDouble.empty();
	private boolean noStem;
	private Optional<PhoneticMatcher> matcher = Optional.empty();
	private boolean withSuffixTrie;

	private TextField(Builder<K> builder) {
		super(Type.TEXT, builder);
		this.noStem = builder.noStem;
		this.weight = builder.weight;
		this.matcher = builder.matcher;
		this.withSuffixTrie = builder.withSuffixTrie;
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

	public boolean isWithSuffixTrie() {
		return withSuffixTrie;
	}

	public void setWithSuffixTrie(boolean withSuffixTrie) {
		this.withSuffixTrie = withSuffixTrie;
	}

	@Override
	public String toString() {
		return "TextField [weight=" + weight + ", noStem=" + noStem + ", matcher=" + matcher + ", withSuffixTrie="
				+ withSuffixTrie + ", type=" + type + ", name=" + name + ", as=" + as + ", sortable=" + sortable
				+ ", unNormalizedForm=" + unNormalizedForm + ", noIndex=" + noIndex + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(matcher, noStem, weight, withSuffixTrie);
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
		TextField<?> other = (TextField<?>) obj;
		return Objects.equals(matcher, other.matcher) && noStem == other.noStem && Objects.equals(weight, other.weight)
				&& withSuffixTrie == other.withSuffixTrie;
	}

	@Override
	protected void buildField(SearchCommandArgs<K, Object> args) {
		args.add(SearchCommandKeyword.TEXT);
		if (noStem) {
			args.add(SearchCommandKeyword.NOSTEM);
		}
		weight.ifPresent(w -> args.add(SearchCommandKeyword.WEIGHT).add(w));
		matcher.ifPresent(m -> args.add(SearchCommandKeyword.PHONETIC).add(m.getCode()));
		if (withSuffixTrie) {
			args.add(SearchCommandKeyword.WITHSUFFIXTRIE);
		}
	}

	public static <K> Builder<K> name(K name) {
		return new Builder<>(name);
	}

	public static class Builder<K> extends Field.Builder<K, Builder<K>> {

		private boolean noStem;
		private OptionalDouble weight = OptionalDouble.empty();
		private Optional<PhoneticMatcher> matcher = Optional.empty();
		private boolean withSuffixTrie;

		public Builder(K name) {
			super(name);
		}

		public Builder<K> noStem() {
			return noStem(true);
		}

		public Builder<K> noStem(boolean noStem) {
			this.noStem = noStem;
			return this;
		}

		public Builder<K> weight(double weight) {
			this.weight = OptionalDouble.of(weight);
			return this;
		}

		public Builder<K> matcher(PhoneticMatcher matcher) {
			this.matcher = Optional.of(matcher);
			return this;
		}

		public Builder<K> withSuffixTrie() {
			return withSuffixTrie(true);
		}

		public Builder<K> withSuffixTrie(boolean withSuffixTrie) {
			this.withSuffixTrie = withSuffixTrie;
			return this;
		}

		public TextField<K> build() {
			return new TextField<>(this);
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