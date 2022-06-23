package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

public class AddOptions<K, V> extends AbstractCreateOptions<K, V> {

	private AddOptions(Builder<K, V, ?> builder) {
		super(builder);
	}

	@Override
	protected TimeSeriesCommandKeyword getDuplicatePolicyKeyword() {
		return TimeSeriesCommandKeyword.ON_DUPLICATE;
	}

	public static <K, V> Builder<K, V, AddOptions<K, V>> builder() {
		return new Builder<>(AddOptions::new);
	}

}
