package com.redis.lettucemod.timeseries;

import java.time.Duration;
import java.util.Optional;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.CommandArgs;

public class Aggregation implements CompositeArgument {

	private final Aggregator aggregator;
	private final Duration bucketDuration;
	private Optional<Align> align = Optional.empty();
	private Optional<BucketTimestamp> bucketTimestamp = Optional.empty();
	private boolean empty;

	public Aggregation(Aggregator aggregator, Duration bucketDuration) {
		this.aggregator = aggregator;
		this.bucketDuration = bucketDuration;
	}

	private Aggregation(Builder builder) {
		this.align = builder.align;
		this.aggregator = builder.aggregator;
		this.bucketDuration = builder.bucketDuration;
		this.bucketTimestamp = builder.bucketTimestamp;
		this.empty = builder.empty;
	}

	public Aggregator getAggregator() {
		return aggregator;
	}

	public Duration getBucketDuration() {
		return bucketDuration;
	}

	public Optional<Align> getAlign() {
		return align;
	}

	public void setAlign(Optional<Align> align) {
		this.align = align;
	}

	public Optional<BucketTimestamp> getBucketTimestamp() {
		return bucketTimestamp;
	}

	public void setBucketTimestamp(Optional<BucketTimestamp> bucketTimestamp) {
		this.bucketTimestamp = bucketTimestamp;
	}

	public boolean isEmpty() {
		return empty;
	}

	public void setEmpty(boolean empty) {
		this.empty = empty;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		align.ifPresent(a -> a.build(args));
		aggregator.build(args);
		args.add(bucketDuration.toMillis());
		bucketTimestamp.ifPresent(bt -> bt.build(args));
		if (empty) {
			args.add(TimeSeriesCommandKeyword.EMPTY);
		}
	}

	@Override
	public String toString() {
		return "Aggregation [aggregator=" + aggregator + ", bucketDuration=" + bucketDuration + ", align=" + align
				+ ", bucketTimestamp=" + bucketTimestamp + ", empty=" + empty + "]";
	}

	public static class Align implements CompositeArgument {

		private static final Align START = new Align(0);
		private static final Align END = new Align(0);

		private final long value;

		private Align(long value) {
			this.value = value;
		}

		public static Align of(long timestamp) {
			return new Align(timestamp);
		}

		public static Align start() {
			return START;
		}

		public static Align end() {
			return END;
		}

		@Override
		public <K, V> void build(CommandArgs<K, V> args) {
			args.add(TimeSeriesCommandKeyword.ALIGN);
			if (this == START) {
				args.add(TimeSeriesCommandKeyword.START);
			} else {
				if (this == END) {
					args.add(TimeSeriesCommandKeyword.END);
				} else {
					args.add(value);
				}
			}
		}

	}

	public enum BucketTimestamp implements CompositeArgument {

		LOW(TimeSeriesCommandKeyword.START), HIGH(TimeSeriesCommandKeyword.END), MID(TimeSeriesCommandKeyword.MID);

		private final TimeSeriesCommandKeyword keyword;

		private BucketTimestamp(TimeSeriesCommandKeyword keyword) {
			this.keyword = keyword;
		}

		@Override
		public <K, V> void build(CommandArgs<K, V> args) {
			args.add(TimeSeriesCommandKeyword.BUCKETTIMESTAMP);
			args.add(keyword);
		}
	}

	public static BucketDurationBuilder aggregator(Aggregator aggregator) {
		return new BucketDurationBuilder(aggregator);
	}

	public static class BucketDurationBuilder {
		private final Aggregator aggregator;

		public BucketDurationBuilder(Aggregator aggregator) {
			this.aggregator = aggregator;
		}

		public Builder bucketDuration(Duration duration) {
			LettuceAssert.notNull(duration, "Bucket duration must not be null");
			LettuceAssert.isTrue(!duration.isNegative() && !duration.isZero(), "Bucket duration must be positive");
			return new Builder(aggregator, duration);
		}
	}

	public static class Builder {

		private final Aggregator aggregator;
		private final Duration bucketDuration;
		private Optional<Align> align = Optional.empty();
		private Optional<BucketTimestamp> bucketTimestamp = Optional.empty();
		private boolean empty;

		private Builder(Aggregator aggregator, Duration bucketDuration) {
			this.aggregator = aggregator;
			this.bucketDuration = bucketDuration;
		}

		public Builder align(Align align) {
			this.align = Optional.of(align);
			return this;
		}

		public Builder bucketTimestamp(BucketTimestamp bt) {
			this.bucketTimestamp = Optional.of(bt);
			return this;
		}

		public Builder empty() {
			return empty(true);
		}

		public Builder empty(boolean empty) {
			this.empty = empty;
			return this;
		}

		public Aggregation build() {
			return new Aggregation(this);
		}
	}

}
