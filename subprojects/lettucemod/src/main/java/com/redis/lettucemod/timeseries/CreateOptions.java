package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.protocol.CommandArgs;

public class CreateOptions<K, V> extends AbstractAddCreateOptions<K, V> {

	private CreateOptions(Builder<K, V> builder) {
		super(builder);
	}

	@Override
	public <L, W> void build(CommandArgs<L, W> args) {
		buildRetentionPeriod(args);
		buildEncoding(args);
		buildChunkSize(args);
		buildDuplicatePolicy(args, TimeSeriesCommandKeyword.DUPLICATE_POLICY);
		buildLabels(args);
	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	public static class Builder<K, V> extends AbstractAddCreateOptions.Builder<K, V, Builder<K, V>> {

		public CreateOptions<K, V> build() {
			return new CreateOptions<>(this);
		}

	}

}
