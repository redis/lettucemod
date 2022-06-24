package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

public class AddOptions<K, V> extends AbstractCreateOptions<K, V> {

	private AddOptions(Builder<K, V> builder) {
		super(builder);
	}

	@Override
	protected TimeSeriesCommandKeyword getDuplicatePolicyKeyword() {
		return TimeSeriesCommandKeyword.ON_DUPLICATE;
	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	public static class Builder<K, V> extends AbstractCreateOptions.Builder<K, V, Builder<K, V>> {

		public AddOptions<K, V> build() {
			return new AddOptions<>(this);
		}

	}

}
