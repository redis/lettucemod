package com.redis.lettucemod.timeseries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

abstract class AbstractAddAlterCreateIncrbyOptions<K, V> implements CompositeArgument {

	private final OptionalLong retentionPeriod;
	private final OptionalLong chunkSize;
	private final List<Label<K, V>> labels;

	protected AbstractAddAlterCreateIncrbyOptions(Builder<K, V, ?> builder) {
		this.retentionPeriod = builder.retentionTime;
		this.chunkSize = builder.chunkSize;
		this.labels = builder.labels;
	}

	protected <L, W> void buildRetentionPeriod(CommandArgs<L, W> args) {
		retentionPeriod.ifPresent(t -> args.add(TimeSeriesCommandKeyword.RETENTION).add(t));
	}

	protected <L, W> void buildChunkSize(CommandArgs<L, W> args) {
		chunkSize.ifPresent(s -> args.add(TimeSeriesCommandKeyword.CHUNK_SIZE).add(s));
	}

	@SuppressWarnings("unchecked")
	protected <L, W> void buildLabels(CommandArgs<L, W> args) {
		if (!labels.isEmpty()) {
			args.add(TimeSeriesCommandKeyword.LABELS);
			labels.forEach(l -> args.addKey((L) l.getLabel()).addValue((W) l.getValue()));
		}
	}

	public enum Encoding {

		COMPRESSED(TimeSeriesCommandKeyword.COMPRESSED), UNCOMPRESSED(TimeSeriesCommandKeyword.UNCOMPRESSED);

		private TimeSeriesCommandKeyword keyword;

		Encoding(TimeSeriesCommandKeyword keyword) {
			this.keyword = keyword;
		}

		public TimeSeriesCommandKeyword getKeyword() {
			return keyword;
		}

	}

	@SuppressWarnings("unchecked")
	public static class Builder<K, V, B extends Builder<K, V, B>> {
		private OptionalLong retentionTime = OptionalLong.empty();
		private OptionalLong chunkSize = OptionalLong.empty();
		private List<Label<K, V>> labels = new ArrayList<>();

		public B retentionPeriod(long retentionPeriod) {
			this.retentionTime = OptionalLong.of(retentionPeriod);
			return (B) this;
		}

		public B chunkSize(long chunkSize) {
			this.chunkSize = OptionalLong.of(chunkSize);
			return (B) this;
		}

		public B labels(Label<K, V>... labels) {
			this.labels.addAll(Arrays.asList(labels));
			return (B) this;
		}

	}

}
