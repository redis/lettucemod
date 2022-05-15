package com.redis.lettucemod.timeseries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

class BaseRangeOptions implements CompositeArgument {

	public static final long START = Long.MIN_VALUE;
	public static final long END = Long.MAX_VALUE;

	private final long from;
	private final long to;
	private final List<Long> filterByTimestamp;
	private final Optional<FilterByValue> filterByValue;
	private final OptionalLong count;
	private final Optional<Aggregation> aggregation;

	protected BaseRangeOptions(Builder<?> builder) {
		this.from = builder.from;
		this.to = builder.to;
		this.filterByTimestamp = builder.filterByTimestamp;
		this.filterByValue = builder.filterByValue;
		this.count = builder.count;
		this.aggregation = builder.aggregation;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		buildFromToFilterBys(args);
		buildCountAlign(args);
	}

	protected <K, V> void buildCountAlign(CommandArgs<K, V> args) {
		count.ifPresent(c -> args.add(TimeSeriesCommandKeyword.COUNT).add(c));
		aggregation.ifPresent(a -> a.build(args));
	}

	protected <K, V> void buildFromToFilterBys(CommandArgs<K, V> args) {
		if (from == START) {
			args.add(TimeSeriesCommandKeyword.START);
		} else {
			args.add(from);
		}
		if (to == END) {
			args.add(TimeSeriesCommandKeyword.END);
		} else {
			args.add(to);
		}
		if (!filterByTimestamp.isEmpty()) {
			args.add(TimeSeriesCommandKeyword.FILTER_BY_TS);
			filterByTimestamp.forEach(args::add);
		}
		filterByValue.ifPresent(f -> f.build(args));
	}

	@SuppressWarnings("unchecked")
	static class Builder<B extends Builder<B>> {

		private final long from;
		private final long to;
		private OptionalLong count = OptionalLong.empty();
		private Optional<Aggregation> aggregation = Optional.empty();
		private List<Long> filterByTimestamp = new ArrayList<>();
		private Optional<FilterByValue> filterByValue = Optional.empty();

		public Builder(long from, long to) {
			this.from = from;
			this.to = to;
		}

		public B filterByTimestamp(long... timestamps) {
			filterByTimestamp = Arrays.stream(timestamps).boxed().collect(Collectors.toList());
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