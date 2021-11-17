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

	public static IToStage from(long from) {
		return new Builder().from(from);
	}

	public interface IFromStage {
		public IToStage from(long from);
	}

	public interface IToStage {
		public IBuildStage to(long to);
	}

	public interface IBuildStage {
		public IBuildStage count(Long count);

		public IBuildStage aggregation(Aggregation aggregation);

		public RangeOptions build();
	}

	public static final class Builder implements IFromStage, IToStage, IBuildStage {
		private long from;
		private long to;
		private Long count;
		private Aggregation aggregation;

		private Builder() {
		}

		@Override
		public IToStage from(long from) {
			this.from = from;
			return this;
		}

		@Override
		public IBuildStage to(long to) {
			this.to = to;
			return this;
		}

		@Override
		public IBuildStage count(Long count) {
			this.count = count;
			return this;
		}

		@Override
		public IBuildStage aggregation(Aggregation aggregation) {
			this.aggregation = aggregation;
			return this;
		}

		@Override
		public RangeOptions build() {
			return new RangeOptions(this);
		}
	}

}
