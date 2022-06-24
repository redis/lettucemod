package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

public class CreateOptions<K, V> extends AbstractCreateOptions<K, V> {

	private CreateOptions(Builder<K, V> builder) {
		super(builder);
	}

	@Override
	protected TimeSeriesCommandKeyword getDuplicatePolicyKeyword() {
		return TimeSeriesCommandKeyword.DUPLICATE_POLICY;
	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	public static class Builder<K, V> extends AbstractCreateOptions.Builder<K, V, Builder<K, V>> {

		public CreateOptions<K, V> build() {
			return new CreateOptions<>(this);
		}

	}

}
