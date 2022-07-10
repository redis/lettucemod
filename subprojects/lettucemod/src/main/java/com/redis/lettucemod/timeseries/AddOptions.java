package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.protocol.CommandArgs;

public class AddOptions<K, V> extends AbstractAddCreateOptions<K, V> {

	private AddOptions(Builder<K, V> builder) {
		super(builder);
	}

	@Override
	public <L, W> void build(CommandArgs<L, W> args) {
		buildRetentionPeriod(args);
		buildEncoding(args);
		buildChunkSize(args);
		buildDuplicatePolicy(args, TimeSeriesCommandKeyword.ON_DUPLICATE);
		buildLabels(args);
	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	public static class Builder<K, V> extends AbstractAddCreateOptions.Builder<K, V, Builder<K, V>> {

		public AddOptions<K, V> build() {
			return new AddOptions<>(this);
		}

	}

}
