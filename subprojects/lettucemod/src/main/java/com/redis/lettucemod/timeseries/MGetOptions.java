package com.redis.lettucemod.timeseries;

import java.util.Optional;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class MGetOptions<K, V> implements CompositeArgument {

	private Latest latest = new Latest();
	private Optional<Labels<K>> labels = Optional.empty();
	private Filters<V> filters = new Filters<>();

	public MGetOptions() {
	}

	private MGetOptions(Builder<K, V> builder) {
		this.latest = builder.latest;
		this.labels = builder.labels;
		this.filters = builder.filters;
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

	public boolean getLatest() {
		return latest.isEnabled();
	}

	public void setLatest(boolean latest) {
		this.latest = Latest.of(latest);
	}

	@SuppressWarnings("hiding")
	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		latest.build(args);
		labels.ifPresent(l -> l.build(args));
		filters.build(args);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> Builder<K, V> filters(V... filters) {
		return new Builder<>(filters);
	}

	@SuppressWarnings("unchecked")
	public static class Builder<K, V> {

		private Latest latest = new Latest();
		private Optional<Labels<K>> labels = Optional.empty();
		private Filters<V> filters = new Filters<>();

		public Builder(V... filters) {
			this.filters = Filters.of(filters);
		}

		public Builder<K, V> latest() {
			return latest(true);
		}

		public Builder<K, V> latest(boolean latest) {
			this.latest = Latest.of(latest);
			return this;
		}

		public Builder<K, V> filters(V... filters) {
			this.filters = Filters.of(filters);
			return this;
		}

		public Builder<K, V> withLabels(boolean withLabels) {
			this.labels = withLabels ? Optional.of(new Labels<>()) : Optional.empty();
			return this;
		}

		public Builder<K, V> withLabels() {
			return withLabels(true);
		}

		public Builder<K, V> selectedLabels(K... labels) {
			this.labels = Optional.of(Labels.of(labels));
			return this;
		}

		public MGetOptions<K, V> build() {
			return new MGetOptions<>(this);
		}

	}
}
