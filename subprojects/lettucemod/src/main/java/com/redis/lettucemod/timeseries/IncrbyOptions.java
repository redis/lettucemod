package com.redis.lettucemod.timeseries;

import java.util.OptionalLong;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.protocol.CommandArgs;

public class IncrbyOptions<K, V> extends AbstractAddAlterCreateIncrbyOptions<K, V> {

	private final boolean uncompressed;
	private final OptionalLong timestamp;

	private IncrbyOptions(Builder<K, V> builder) {
		super(builder);
		this.uncompressed = builder.uncompressed;
		this.timestamp = builder.timestamp;
	}

	@Override
	public <L, W> void build(CommandArgs<L, W> args) {
		timestamp
				.ifPresent(t -> TimeSeriesCommandBuilder.addTimestamp(args.add(TimeSeriesCommandKeyword.TIMESTAMP), t));
		buildRetentionPeriod(args);
		if (uncompressed) {
			args.add(TimeSeriesCommandKeyword.UNCOMPRESSED);
		}
		buildChunkSize(args);
		buildLabels(args);
	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	public static class Builder<K, V> extends AbstractAddAlterCreateIncrbyOptions.Builder<K, V, Builder<K, V>> {

		private OptionalLong timestamp = OptionalLong.empty();
		private boolean uncompressed;

		public IncrbyOptions<K, V> build() {
			return new IncrbyOptions<>(this);
		}

		public Builder<K, V> uncompressed(boolean uncompressed) {
			this.uncompressed = uncompressed;
			return this;
		}

		public Builder<K, V> timestamp(long timestamp) {
			this.timestamp = OptionalLong.of(timestamp);
			return this;
		}

		public Builder<K, V> autoTimestamp() {
			this.timestamp = OptionalLong.of(Sample.AUTO_TIMESTAMP);
			return this;
		}

	}

}
