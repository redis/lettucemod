package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class AggregateOptions<K, V> implements RediSearchArgument<K, V> {

	private final List<AggregateOperation<K, V>> operations;
	private final List<String> loads;
	private final boolean verbatim;

	public AggregateOptions(List<AggregateOperation<K, V>> operations, List<String> loads, boolean verbatim) {
		this.operations = operations;
		this.loads = loads;
		this.verbatim = verbatim;
	}

	@Override
	public void build(SearchCommandArgs<K, V> args) {
		if (verbatim) {
			args.add(SearchCommandKeyword.VERBATIM);
		}
		if (!loads.isEmpty()) {
			args.add(SearchCommandKeyword.LOAD);
			args.add(loads.size());
			for (String load : loads) {
				args.addProperty(load);
			}
		}
		for (AggregateOperation<K, V> operation : operations) {
			operation.build(args);
		}
	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	public static <K, V> Builder<K, V> apply(Apply<K, V> apply) {
		return operation(apply);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> Builder<K, V> filter(Filter<V> filter) {
		return operation(filter);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> Builder<K, V> group(Group group) {
		return operation(group);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> Builder<K, V> limit(Limit limit) {
		return operation(limit);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> Builder<K, V> sort(Sort sort) {
		return operation(sort);
	}

	public static <K, V> Builder<K, V> operation(AggregateOperation<K, V> operation) {
		return new Builder<>(operation);
	}

	public static class Builder<K, V> {

		private final List<AggregateOperation<K, V>> operations = new ArrayList<>();
		private final List<String> loads = new ArrayList<>();
		private boolean verbatim;

		public Builder() {
		}

		public Builder(AggregateOperation<K, V> operation) {
			operations.add(operation);
		}

		public Builder<K, V> apply(Apply<K, V> apply) {
			this.operations.add(apply);
			return this;
		}

		@SuppressWarnings("unchecked")
		public Builder<K, V> filter(Filter<V> filter) {
			this.operations.add(filter);
			return this;

		}

		@SuppressWarnings("unchecked")
		public Builder<K, V> group(Group group) {
			this.operations.add(group);
			return this;

		}

		@SuppressWarnings("unchecked")
		public Builder<K, V> limit(Limit limit) {
			this.operations.add(limit);
			return this;

		}

		@SuppressWarnings("unchecked")
		public Builder<K, V> sort(Sort sort) {
			this.operations.add(sort);
			return this;

		}

		public Builder<K, V> load(String load) {
			this.loads.add(load);
			return this;
		}

		public Builder<K, V> loads(String... loads) {
			this.loads.addAll(Arrays.asList(loads));
			return this;
		}

		public Builder<K, V> verbatim(boolean verbatim) {
			this.verbatim = verbatim;
			return this;
		}

		public AggregateOptions<K, V> build() {
			return new AggregateOptions<>(operations, loads, verbatim);
		}

	}

}
