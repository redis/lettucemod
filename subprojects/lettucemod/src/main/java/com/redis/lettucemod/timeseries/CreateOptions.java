package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

public class CreateOptions<K, V> extends AbstractCreateOptions<K, V> {

	private CreateOptions(Builder<K, V, ?> builder) {
		super(builder);
	}

	@Override
	protected TimeSeriesCommandKeyword getDuplicatePolicyKeyword() {
		return TimeSeriesCommandKeyword.DUPLICATE_POLICY;
	}

	public static <K, V> Builder<K, V, CreateOptions<K, V>> builder() {
		return new Builder<>(CreateOptions::new);
	}

}
