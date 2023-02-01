package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

public class AddOptions<K, V> extends BaseCreateOptions<K, V> {

	private AddOptions(Builder<K, V> builder) {
		super(TimeSeriesCommandKeyword.ON_DUPLICATE, builder);
	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	public static class Builder<K, V> extends BaseCreateOptions.Builder<K, V, Builder<K, V>> {

		public AddOptions<K, V> build() {
			return new AddOptions<>(this);
		}

	}

}
