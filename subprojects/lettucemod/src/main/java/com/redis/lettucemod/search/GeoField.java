package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class GeoField<K> extends Field<K> {

	private GeoField(Builder<K> builder) {
		super(Type.GEO, builder);
	}

	@Override
	protected void buildField(SearchCommandArgs<K, Object> args) {
		args.add(SearchCommandKeyword.GEO);
	}

	public static <K> Builder<K> name(K name) {
		return new Builder<>(name);
	}

	public static class Builder<K> extends Field.Builder<K, Builder<K>> {

		public Builder(K name) {
			super(name);
		}

		public GeoField<K> build() {
			return new GeoField<>(this);
		}

	}
}