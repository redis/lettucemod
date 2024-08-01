package com.redis.lettucemod.timeseries;

import java.util.Optional;
import java.util.OptionalLong;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class AbstractRangeOptions implements CompositeArgument {

	private Latest latest = new Latest();
	private Optional<long[]> filterByTimestamp = Optional.empty();
	private Optional<FilterByValue> filterByValue = Optional.empty();
	private OptionalLong count = OptionalLong.empty();
	private Optional<Aggregation> aggregation = Optional.empty();

	protected AbstractRangeOptions() {
	}

	protected AbstractRangeOptions(Builder<?> builder) {
		this.latest = builder.latest;
		this.filterByTimestamp = builder.filterByTimestamp;
		this.filterByValue = builder.filterByValue;
		this.count = builder.count;
		this.aggregation = builder.aggregation;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		latest.build(args);
		filterByTimestamp.ifPresent(f -> {
			args.add(TimeSeriesCommandKeyword.FILTER_BY_TS);
			for (long ts : f) {
				args.add(ts);
			}
		});
		filterByValue.ifPresent(f -> f.build(args));
		count.ifPresent(c -> args.add(TimeSeriesCommandKeyword.COUNT).add(c));
		aggregation.ifPresent(a -> a.build(args));
	}

	public void setLatest(boolean latest) {
		this.latest = Latest.of(latest);
	}

	public boolean isLatest() {
		return latest.isEnabled();
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

	@SuppressWarnings("unchecked")
	public static class Builder<B extends Builder<B>> {

		private Latest latest = new Latest();
		private Optional<long[]> filterByTimestamp = Optional.empty();
		private Optional<FilterByValue> filterByValue = Optional.empty();
		private OptionalLong count = OptionalLong.empty();
		private Optional<Aggregation> aggregation = Optional.empty();

		public B latest() {
			return latest(true);
		}

		public B latest(boolean latest) {
			this.latest = Latest.of(latest);
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