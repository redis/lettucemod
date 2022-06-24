package com.redis.lettucemod.timeseries;

import java.util.Optional;
import java.util.OptionalLong;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

abstract class BaseRangeOptions implements CompositeArgument {

	private final Optional<long[]> filterByTimestamp;
	private final Optional<FilterByValue> filterByValue;
	private final OptionalLong count;
	private final Optional<Aggregation> aggregation;

	protected BaseRangeOptions(Builder<?> builder) {
		this.filterByTimestamp = builder.filterByTimestamp;
		this.filterByValue = builder.filterByValue;
		this.count = builder.count;
		this.aggregation = builder.aggregation;
	}

	protected <K, V> void buildCount(CommandArgs<K, V> args) {
		count.ifPresent(c -> args.add(TimeSeriesCommandKeyword.COUNT).add(c));
	}

	protected <K, V> void buildAggregation(CommandArgs<K, V> args) {
		aggregation.ifPresent(a -> a.build(args));
	}

	protected <K, V> void buildFilterByTimestamp(CommandArgs<K, V> args) {
		filterByTimestamp.ifPresent(f -> {
			args.add(TimeSeriesCommandKeyword.FILTER_BY_TS);
			for (long ts : f) {
				args.add(ts);
			}
		});
	}

	protected <K, V> void buildFilterByValue(CommandArgs<K, V> args) {
		filterByValue.ifPresent(f -> f.build(args));
	}

	@SuppressWarnings("unchecked")
	public static class Builder<B extends Builder<B>> {

		private Optional<long[]> filterByTimestamp = Optional.empty();
		private Optional<FilterByValue> filterByValue = Optional.empty();
		private OptionalLong count = OptionalLong.empty();
		private Optional<Aggregation> aggregation = Optional.empty();

		public B filterByTimestamp(long... timestamps) {
			filterByTimestamp = Optional.of(timestamps);
			return (B) this;
		}

		public B filterByValue(double min, double max) {
			filterByValue = Optional.of(FilterByValue.of(min, max));
			return (B) this;
		}

		public B count(long count) {
			this.count = OptionalLong.of(count);
			return (B) this;
		}

		public B aggregation(Aggregation aggregation) {
			this.aggregation = Optional.of(aggregation);
			return (B) this;
		}

	}

	public static class FilterByValue implements CompositeArgument {

		private double min;
		private double max;

		public double getMin() {
			return min;
		}

		public void setMin(double min) {
			this.min = min;
		}

		public double getMax() {
			return max;
		}

		public void setMax(double max) {
			this.max = max;
		}

		public static FilterByValue of(double min, double max) {
			FilterByValue filter = new FilterByValue();
			filter.setMin(min);
			filter.setMax(max);
			return filter;
		}

		@Override
		public <K, V> void build(CommandArgs<K, V> args) {
			args.add(TimeSeriesCommandKeyword.FILTER_BY_VALUE);
			args.add(min);
			args.add(max);
		}

	}
}