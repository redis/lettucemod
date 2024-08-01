package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class NumericField<K> extends Field<K> {

	private NumericField(Builder<K> builder) {
		super(Type.NUMERIC, builder);
	}

	@Override
	protected void buildField(SearchCommandArgs<K, Object> args) {
		args.add(SearchCommandKeyword.NUMERIC);
	}

	@Override
	public String toString() {
		return "NumericField [type=" + type + ", name=" + name + ", as=" + as + ", sortable=" + sortable
				+ ", unNormalizedForm=" + unNormalizedForm + ", noIndex=" + noIndex + "]";
	}

	public static <K> Builder<K> name(K name) {
		return new Builder<>(name);
	}

	public static class Builder<K> extends Field.Builder<K, Builder<K>> {

		public Builder(K name) {
			super(name);
		}

		public NumericField<K> build() {
			return new NumericField<>(this);
		}

	}
}