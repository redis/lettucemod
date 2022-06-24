package com.redis.lettucemod.timeseries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class MRangeOptions<K, V> extends BaseRangeOptions {

	private final Optional<List<K>> withLabels;
	private final List<V> filters;
	private final Optional<GroupBy<K>> groupBy;

	private MRangeOptions(Builder<K, V> builder) {
		super(builder);
		this.withLabels = builder.withLabels;
		this.filters = builder.filters;
		this.groupBy = builder.groupBy;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <L, W> void build(CommandArgs<L, W> args) {
		buildFilterByTimestamp(args);
		buildFilterByValue(args);
		withLabels.ifPresent(labels -> {
			if (labels.isEmpty()) {
				args.add(TimeSeriesCommandKeyword.WITHLABELS);
			} else {
				args.add(TimeSeriesCommandKeyword.SELECTED_LABELS);
				labels.forEach(l -> args.addKey((L) l));
			}
		});
		buildCount(args);
		buildAggregation(args);
		if (!filters.isEmpty()) {
			args.add(TimeSeriesCommandKeyword.FILTER);
			filters.forEach(f -> args.addValue((W) f));
		}
		groupBy.ifPresent(g -> g.build(args));
	}

	public static class GroupBy<K> implements CompositeArgument {

		private final K label;
		private final Reducer reducer;

		private GroupBy(K label, Reducer reducer) {
			this.label = label;
			this.reducer = reducer;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <L, W> void build(CommandArgs<L, W> args) {
			args.add(TimeSeriesCommandKeyword.GROUPBY);
			args.addKey((L) label);
			args.add(TimeSeriesCommandKeyword.REDUCE);
			args.add(reducer.name());
		}

		public static <K> GroupBy<K> of(K label, Reducer reducer) {
			return new GroupBy<>(label, reducer);
		}

	}

	public enum Reducer {
		SUM, MIN, MAX
	}

	@SuppressWarnings("unchecked")
	public static <K, V> Builder<K, V> filters(V... filters) {
		return new Builder<>(filters);
	}

	@SuppressWarnings("unchecked")
	public static class Builder<K, V> extends BaseRangeOptions.Builder<Builder<K, V>> {

		private Optional<List<K>> withLabels = Optional.empty();
		private List<V> filters = new ArrayList<>();
		private Optional<GroupBy<K>> groupBy = Optional.empty();

		public Builder(V... filters) {
			this.filters.addAll(Arrays.asList(filters));
		}

		public Builder<K, V> filters(V... filters) {
			this.filters = Arrays.asList(filters);
			return this;
		}

		public Builder<K, V> withLabels() {
			this.withLabels = Optional.of(new ArrayList<>());
			return this;
		}

		public Builder<K, V> selectedLabels(K... labels) {
			this.withLabels = Optional.of(Arrays.asList(labels));
			return this;
		}

		public Builder<K, V> groupBy(GroupBy<K> groupBy) {
			this.groupBy = Optional.of(groupBy);
			return this;
		}

		public MRangeOptions<K, V> build() {
			return new MRangeOptions<>(this);
		}

	}
}
