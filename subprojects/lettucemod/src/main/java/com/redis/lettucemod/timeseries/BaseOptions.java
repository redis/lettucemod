package com.redis.lettucemod.timeseries;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class BaseOptions<K, V> implements CompositeArgument {

	private Optional<Duration> retentionPeriod = Optional.empty();
	private OptionalLong chunkSize = OptionalLong.empty();
	private List<Label<K, V>> labels = new ArrayList<>();

	public BaseOptions() {
	}

	protected BaseOptions(Builder<K, V, ?> builder) {
		this.retentionPeriod = builder.retentionPeriod;
		this.chunkSize = builder.chunkSize;
		this.labels = builder.labels;
	}

	public List<Label<K, V>> getLabels() {
		return labels;
	}

	public void setLabels(Iterable<Label<K, V>> labels) {
		this.labels = StreamSupport.stream(labels.spliterator(), false).collect(Collectors.toList());
	}

	public OptionalLong getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(OptionalLong chunkSize) {
		this.chunkSize = chunkSize;
	}

	public Optional<Duration> getRetentionPeriod() {
		return retentionPeriod;
	}

	public void setRetentionPeriod(Optional<Duration> retentionPeriod) {
		this.retentionPeriod = retentionPeriod;
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

		private Optional<Duration> retentionPeriod = Optional.empty();
		private OptionalLong chunkSize = OptionalLong.empty();
		private final List<Label<K, V>> labels = new ArrayList<>();

		public B retentionPeriod(long millis) {
			return retentionPeriod(Duration.ofMillis(millis));
		}

		public B retentionPeriod(Duration duration) {
			this.retentionPeriod = Optional.of(duration);
			return (B) this;
		}

		public B chunkSize(long chunkSize) {
			this.chunkSize = OptionalLong.of(chunkSize);
			return (B) this;
		}

		public B labels(Iterable<Label<K, V>> labels) {
			for (Label<K, V> label : labels) {
				this.labels.add(label);
			}
			return (B) this;
		}

		/**
		 * Sets labels with the given key/value pairs.
		 * 
		 * @param keyValues the name/value pairs to add
		 * @return this builder
		 */
		public B labels(Object... keyValues) {
			if (keyValues == null || keyValues.length == 0) {
				return (B) this;
			}
			if (keyValues.length % 2 == 1) {
				throw new IllegalArgumentException("size must be even, it is a set of key=value pairs");
			}
			for (int i = 0; i < keyValues.length; i += 2) {
				label(Label.of((K) keyValues[i], (V) keyValues[i + 1]));
			}
			return (B) this;
		}

		public B label(Label<K, V> label) {
			return labels(label);
		}

		public B labels(Label<K, V>... labels) {
			return labels(Arrays.asList(labels));
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
