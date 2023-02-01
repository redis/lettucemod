package com.redis.lettucemod.timeseries;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class BaseOptions<K, V> implements CompositeArgument {

	private final Optional<Duration> retentionPeriod;
	private final OptionalLong chunkSize;
	private final List<Label<K, V>> labels;

	protected BaseOptions(Builder<K, V, ?> builder) {
		this.retentionPeriod = builder.retentionTime;
		this.chunkSize = builder.chunkSize;
		this.labels = builder.labels;
	}

	@SuppressWarnings({ "hiding", "unchecked" })
	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		retentionPeriod.ifPresent(t -> args.add(TimeSeriesCommandKeyword.RETENTION).add(t.toMillis()));
		chunkSize.ifPresent(s -> args.add(TimeSeriesCommandKeyword.CHUNK_SIZE).add(s));
		if (!labels.isEmpty()) {
			args.add(TimeSeriesCommandKeyword.LABELS);
			labels.forEach(l -> args.addKey((K) l.getLabel()).addValue((V) l.getValue()));
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
		private Optional<Duration> retentionTime = Optional.empty();
		private OptionalLong chunkSize = OptionalLong.empty();
		private final List<Label<K, V>> labels = new ArrayList<>();

		public B retentionPeriod(long millis) {
			return retentionPeriod(Duration.ofMillis(millis));
		}

		public B retentionPeriod(Duration duration) {
			this.retentionTime = Optional.of(duration);
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

		public B labels(Map<K, V> map) {
			map.forEach(this::label);
			return (B) this;
		}

		public B label(K label, V value) {
			this.labels.add(Label.of(label, value));
			return (B) this;
		}

	}

}
