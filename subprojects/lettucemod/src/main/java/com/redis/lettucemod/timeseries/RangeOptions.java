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

public class RangeOptions implements CompositeArgument {

	public static final String MIN_TIMESTAMP = "-";
	public static final String MAX_TIMESTAMP = "+";

	private final long from;
	private final long to;
	private final List<Long> filterByTimestamp;
	private final Optional<FilterByValue> filterByValue;
	private final OptionalLong count;
	private final Optional<Aggregation> aggregation;

	private RangeOptions(Builder builder) {
		this.from = builder.from;
		this.to = builder.to;
		this.filterByTimestamp = builder.filterByTimestamp;
		this.filterByValue = builder.filterByValue;
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
		if (!filterByTimestamp.isEmpty()) {
			args.add(TimeSeriesCommandKeyword.FILTER_BY_TS);
			filterByTimestamp.forEach(args::add);
		}
		filterByValue.ifPresent(f -> f.build(args));
		count.ifPresent(c -> args.add(TimeSeriesCommandKeyword.COUNT).add(c));
		aggregation.ifPresent(a -> a.build(args));
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
		private OptionalLong count = OptionalLong.empty();
		private Optional<Aggregation> aggregation = Optional.empty();
		private List<Long> filterByTimestamp = new ArrayList<>();
		private Optional<FilterByValue> filterByValue = Optional.empty();

		public Builder filterByTimestamp(long... timestamps) {
			filterByTimestamp = Arrays.stream(timestamps).boxed().collect(Collectors.toList());
			return this;
		}

		public Builder filterByValue(double min, double max) {
			filterByValue = Optional.of(FilterByValue.of(min, max));
			return this;
		}

		public Builder from(long fromTimestamp) {
			this.from = fromTimestamp;
			return this;
		}

		public Builder to(long toTimestamp) {
			this.to = toTimestamp;
			return this;
		}

		public Builder count(long count) {
			this.count = OptionalLong.of(count);
			return this;
		}

		public Builder aggregation(Aggregation aggregation) {
			this.aggregation = Optional.of(aggregation);
			return this;
		}

		public RangeOptions build() {
			return new RangeOptions(this);
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
