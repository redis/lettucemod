package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class RangeOptions implements CompositeArgument {

	private static final String MIN_TIMESTAMP = "-";
	private static final String MAX_TIMESTAMP = "+";

	private final long from;
	private final long to;
	private Long count;
	private Aggregation aggregation;

	private RangeOptions(Builder builder) {
		this.from = builder.from;
		this.to = builder.to;
		this.count = builder.count;
		this.aggregation = builder.aggregation;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		if (from == 0) {
			args.add(MIN_TIMESTAMP);
		} else {
			args.add(from);
		}
		if (to == 0) {
			args.add(MAX_TIMESTAMP);
		} else {
			args.add(to);
		}
		if (count != null) {
			args.add(TimeSeriesCommandKeyword.COUNT);
			args.add(count);
		}
		if (aggregation != null) {
			aggregation.build(args);
		}
	}

	public static Builder from(long fromTimestamp) {
		return new Builder().from(fromTimestamp);
	}

	public static Builder to(long toTimestamp) {
		return new Builder().to(toTimestamp);
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private long from;
		private long to;
		private Long count;
		private Aggregation aggregation;

		public Builder from(long fromTimestamp) {
			this.from = fromTimestamp;
			return this;
		}

		public Builder to(long toTimestamp) {
			this.to = toTimestamp;
			return this;
		}

		public Builder count(long count) {
			this.count = count;
			return this;
		}

		public Builder aggregation(Aggregation aggregation) {
			this.aggregation = aggregation;
			return this;
		}

		public RangeOptions build() {
			return new RangeOptions(this);
		}

	}

}
