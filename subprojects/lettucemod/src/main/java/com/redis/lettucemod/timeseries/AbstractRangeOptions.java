package com.redis.lettucemod.timeseries;

import java.util.Optional;
import java.util.OptionalLong;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

abstract class AbstractRangeOptions implements CompositeArgument {

	private boolean latest;
	private Optional<long[]> filterByTimestamp;
	private Optional<FilterByValue> filterByValue;
	private OptionalLong count;
	private Optional<Aggregation> aggregation;

	protected AbstractRangeOptions(Builder<?> builder) {
		this.latest = builder.latest;
		this.filterByTimestamp = builder.filterByTimestamp;
		this.filterByValue = builder.filterByValue;
		this.count = builder.count;
		this.aggregation = builder.aggregation;
	}

	public void setLatest(boolean latest) {
		this.latest = latest;
	}

	public boolean isLatest() {
		return latest;
	}

	public void setFilterByTimestamp(Optional<long[]> filterByTimestamp) {
		this.filterByTimestamp = filterByTimestamp;
	}

	public Optional<long[]> getFilterByTimestamp() {
		return filterByTimestamp;
	}

	public void setFilterByValue(Optional<FilterByValue> filterByValue) {
		this.filterByValue = filterByValue;
	}

	public Optional<FilterByValue> getFilterByValue() {
		return filterByValue;
	}

	public void setCount(OptionalLong count) {
		this.count = count;
	}

	public OptionalLong getCount() {
		return count;
	}

	public Optional<Aggregation> getAggregation() {
		return aggregation;
	}

	public void setAggregation(Optional<Aggregation> aggregation) {
		this.aggregation = aggregation;
	}

	protected <K, V> void buildCount(CommandArgs<K, V> args) {
		count.ifPresent(c -> args.add(TimeSeriesCommandKeyword.COUNT).add(c));
	}

	protected <K, V> void buildAggregation(CommandArgs<K, V> args) {
		aggregation.ifPresent(a -> a.build(args));
	}

	protected <K, V> void buildLatest(CommandArgs<K, V> args) {
		if (latest) {
			args.add(TimeSeriesCommandKeyword.LATEST);
		}
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

		private boolean latest;
		private Optional<long[]> filterByTimestamp = Optional.empty();
		private Optional<FilterByValue> filterByValue = Optional.empty();
		private OptionalLong count = OptionalLong.empty();
		private Optional<Aggregation> aggregation = Optional.empty();

		public B latest() {
			return latest(true);
		}

		public B latest(boolean latest) {
			this.latest = latest;
			return (B) this;
		}

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