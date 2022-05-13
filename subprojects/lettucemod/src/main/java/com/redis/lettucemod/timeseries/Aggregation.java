package com.redis.lettucemod.timeseries;

import java.util.Optional;
import java.util.OptionalLong;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class Aggregation implements CompositeArgument {

	private final Optional<Align> align;
	private final Aggregator aggregator;
	private final long bucketDuration;
	private OptionalLong bucketTimestamp;
	private boolean empty;

	private Aggregation(Builder builder) {
		this.align = builder.align;
		this.aggregator = builder.aggregator;
		this.bucketDuration = builder.bucketDuration;
		this.bucketTimestamp = builder.bucketTimestamp;
		this.empty = builder.empty;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		align.ifPresent(a -> a.build(args));
		args.add(TimeSeriesCommandKeyword.AGGREGATION);
		args.add(aggregator.getName());
		args.add(bucketDuration);
		bucketTimestamp.ifPresent(bt -> args.add(TimeSeriesCommandKeyword.BUCKETTIMESTAMP).add(bt));
		if (empty) {
			args.add(TimeSeriesCommandKeyword.EMPTY);
		}
	}

	public enum Aggregator {

		AVG, SUM, MIN, MAX, RANGE, COUNT, FIRST, LAST, STD_P("STD.P"), STD_S("STD.S"), VAR_P("VAR.P"), VAR_S("VAR.S"),
		TWA;

		private final String name;

		public String getName() {
			return name;
		}

		Aggregator(String name) {
			this.name = name;
		}

		Aggregator() {
			this.name = this.name();
		}

	}

	public static class Align implements CompositeArgument {

		private final long value;

		public Align(long value) {
			this.value = value;
		}

		@Override
		public <K, V> void build(CommandArgs<K, V> args) {
			args.add(TimeSeriesCommandKeyword.ALIGN);
			if (value == Long.MIN_VALUE) {
				args.add(TimeSeriesCommandKeyword.START);
			} else {
				if (value == Long.MAX_VALUE) {
					args.add(TimeSeriesCommandKeyword.END);
				} else {
					args.add(value);
				}
			}
		}

		public static Align start() {
			return new Align(Long.MIN_VALUE);
		}

		public static Align end() {
			return new Align(Long.MAX_VALUE);
		}

		public static Align of(long value) {
			return new Align(value);
		}
	}

	public static Builder builder(Aggregator aggregator, long bucketDuration) {
		return new Builder(aggregator, bucketDuration);
	}

	public static final class Builder {
		private final Aggregator aggregator;
		private final long bucketDuration;
		private Optional<Align> align = Optional.empty();
		private OptionalLong bucketTimestamp = OptionalLong.empty();
		private boolean empty;

		private Builder(Aggregator aggregator, long bucketDuration) {
			this.aggregator = aggregator;
			this.bucketDuration = bucketDuration;
		}

		public Builder align(Align align) {
			this.align = Optional.of(align);
			return this;
		}

		public Builder bucketTimestamp(long bucketTimestamp) {
			this.bucketTimestamp = OptionalLong.of(bucketTimestamp);
			return this;
		}

		public Builder empty() {
			this.empty = true;
			return this;
		}

		public Aggregation build() {
			return new Aggregation(this);
		}
	}

}
