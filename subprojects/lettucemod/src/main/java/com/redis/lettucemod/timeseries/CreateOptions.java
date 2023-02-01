package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

public class CreateOptions<K, V> extends BaseCreateOptions<K, V> {

	private CreateOptions(Builder<K, V> builder) {
		super(TimeSeriesCommandKeyword.DUPLICATE_POLICY, builder);
	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	public static class Builder<K, V> extends BaseCreateOptions.Builder<K, V, Builder<K, V>> {

		public CreateOptions<K, V> build() {
			return new CreateOptions<>(this);
		}

	}

}
