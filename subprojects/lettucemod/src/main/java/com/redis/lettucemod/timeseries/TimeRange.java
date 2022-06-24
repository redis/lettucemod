package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class TimeRange implements CompositeArgument {

	private final Timestamp from;
	private final Timestamp to;

	private TimeRange(Builder builder) {
		this.from = builder.from;
		this.to = builder.to;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		if (from.isUnbounded()) {
			args.add(TimeSeriesCommandKeyword.START);
		} else {
			args.add(from.getValue());
		}
		if (to.isUnbounded()) {
			args.add(TimeSeriesCommandKeyword.END);
		} else {
			args.add(to.getValue());
		}
	}

	public static Builder from(long timestamp) {
		return builder().from(timestamp);
	}

	public static Builder to(long timestamp) {
		return builder().to(timestamp);
	}

	public static TimeRange unbounded() {
		return builder().build();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		private Timestamp from = Timestamp.unbounded();
		private Timestamp to = Timestamp.unbounded();

		public Builder from(long timestamp) {
			this.from = Timestamp.of(timestamp);
			return this;
		}

		public Builder to(long timestamp) {
			this.to = Timestamp.of(timestamp);
			return this;
		}

		public TimeRange build() {
			return new TimeRange(this);
		}
	}

}
