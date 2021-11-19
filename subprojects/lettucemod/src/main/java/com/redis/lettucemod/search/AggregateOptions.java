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

	public static <K, V> AggregateOptionsBuilder<K, V> builder() {
		return new AggregateOptionsBuilder<>();
	}

	public static <K, V> AggregateOptionsBuilder<K, V> apply(Apply<K, V> apply) {
		return operation(apply);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> AggregateOptionsBuilder<K, V> filter(Filter<V> filter) {
		return operation(filter);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> AggregateOptionsBuilder<K, V> group(Group group) {
		return operation(group);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> AggregateOptionsBuilder<K, V> limit(Limit limit) {
		return operation(limit);
	}

	@SuppressWarnings("unchecked")
	public static <K, V> AggregateOptionsBuilder<K, V> sort(Sort sort) {
		return operation(sort);
	}

	public static <K, V> AggregateOptionsBuilder<K, V> operation(AggregateOperation<K, V> operation) {
		return new AggregateOptionsBuilder<>(operation);
	}

	public static class AggregateOptionsBuilder<K, V> {

		private final List<AggregateOperation<K, V>> operations = new ArrayList<>();
		private final List<String> loads = new ArrayList<>();
		private boolean verbatim;

		public AggregateOptionsBuilder() {
		}

		public AggregateOptionsBuilder(AggregateOperation<K, V> operation) {
			operations.add(operation);
		}

		public AggregateOptionsBuilder<K, V> apply(Apply<K, V> apply) {
			this.operations.add(apply);
			return this;
		}

		@SuppressWarnings("unchecked")
		public AggregateOptionsBuilder<K, V> filter(Filter<V> filter) {
			this.operations.add(filter);
			return this;

		}

		@SuppressWarnings("unchecked")
		public AggregateOptionsBuilder<K, V> group(Group group) {
			this.operations.add(group);
			return this;

		}

		@SuppressWarnings("unchecked")
		public AggregateOptionsBuilder<K, V> limit(Limit limit) {
			this.operations.add(limit);
			return this;

		}

		@SuppressWarnings("unchecked")
		public AggregateOptionsBuilder<K, V> sort(Sort sort) {
			this.operations.add(sort);
			return this;

		}

		public AggregateOptionsBuilder<K, V> load(String load) {
			this.loads.add(load);
			return this;
		}

		public AggregateOptionsBuilder<K, V> loads(String... loads) {
			this.loads.addAll(Arrays.asList(loads));
			return this;
		}

		public AggregateOptionsBuilder<K, V> verbatim(boolean verbatim) {
			this.verbatim = verbatim;
			return this;
		}

		public AggregateOptions<K, V> build() {
			return new AggregateOptions<>(operations, loads, verbatim);
		}

	}

}
