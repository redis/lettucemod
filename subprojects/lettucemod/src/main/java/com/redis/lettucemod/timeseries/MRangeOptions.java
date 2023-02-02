package com.redis.lettucemod.timeseries;

import java.util.Optional;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class MRangeOptions<K, V> extends AbstractRangeOptions {

	private Optional<Labels<K>> labels = Optional.empty();
	private Filters<V> filters = new Filters<>();
	private Optional<GroupBy<K>> groupBy = Optional.empty();

	public MRangeOptions() {
	}

	private MRangeOptions(Builder<K, V> builder) {
		super(builder);
		this.labels = builder.labels;
		this.filters = builder.filters;
		this.groupBy = builder.groupBy;
	}

	public Optional<Labels<K>> getLabels() {
		return labels;
	}

	public void setLabels(Optional<Labels<K>> labels) {
		this.labels = labels;
	}

	public Filters<V> getFilters() {
		return filters;
	}

	public void setFilters(Filters<V> filters) {
		this.filters = filters;
	}

	public Optional<GroupBy<K>> getGroupBy() {
		return groupBy;
	}

	public void setGroupBy(Optional<GroupBy<K>> groupBy) {
		this.groupBy = groupBy;
	}

	@SuppressWarnings("hiding")
	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		super.build(args);
		labels.ifPresent(l -> l.build(args));
		filters.build(args);
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
	public static class Builder<K, V> extends AbstractRangeOptions.Builder<Builder<K, V>> {

		private Optional<Labels<K>> labels = Optional.empty();
		private Filters<V> filters = new Filters<>();
		private Optional<GroupBy<K>> groupBy = Optional.empty();

		public Builder(V... filters) {
			this.filters = Filters.of(filters);
		}

		public Builder<K, V> filters(V... filters) {
			this.filters = Filters.of(filters);
			return this;
		}

		public Builder<K, V> withLabels() {
			this.labels = Optional.of(new Labels<>());
			return this;
		}

		public Builder<K, V> selectedLabels(K... labels) {
			this.labels = Optional.of(Labels.of(labels));
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
