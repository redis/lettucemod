package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class AggregateOptions<K, V> implements RediSearchArgument<K, V> {

	private static final Load LOAD_ALL = Load.identifier("*").build();

	private final List<AggregateOperation<K, V>> operations;
	private final List<Load> loads;
	private final boolean verbatim;
	private final OptionalLong timeout;

	private AggregateOptions(Builder<K, V> builder) {
		this.operations = builder.operations;
		this.loads = builder.loads;
		this.verbatim = builder.verbatim;
		this.timeout = builder.timeout;
	}

	@Override
	public void build(SearchCommandArgs<K, V> args) {
		if (verbatim) {
			args.add(SearchCommandKeyword.VERBATIM);
		}
		if (!loads.isEmpty()) {
			args.add(SearchCommandKeyword.LOAD);
			if (loads.size() == 1 && loads.get(0) == LOAD_ALL) {
				args.add(LOAD_ALL.identifier);
			} else {
				args.add(loads.stream().mapToInt(Load::getNargs).sum());
				loads.forEach(l -> l.build(args));
			}
		}
		operations.forEach(op -> op.build(args));
		timeout.ifPresent(t -> args.add(SearchCommandKeyword.TIMEOUT).add(t));
	}

	@Override
	public String toString() {
		final StringBuilder string = new StringBuilder("AggregateOptions [");
		string.append("operations=").append(operations);
		string.append(", loads=").append(loads);
		string.append(", verbatim=").append(verbatim);
		timeout.ifPresent(t -> string.append(", timeout=").append(t));
		string.append("]");
		return string.toString();
	}

	@SuppressWarnings("rawtypes")
	public static class Load implements RediSearchArgument {

		private final String identifier;
		private final Optional<As> as;

		private Load(Builder builder) {
			this.identifier = builder.identifier;
			this.as = builder.as;
		}

		public int getNargs() {
			if (as.isEmpty()) {
				return 1;
			}
			return 3;
		}

		public static Builder identifier(String identifier) {
			return new Builder(identifier);
		}

		public static class Builder {

			private final String identifier;
			private Optional<As> as = Optional.empty();

			public Builder(String identifier) {
				this.identifier = identifier;
			}

			public Builder as(String field) {
				as = Optional.of(new As(field));
				return this;
			}

			public Load build() {
				return new Load(this);
			}
		}

		@Override
		public void build(SearchCommandArgs args) {
			args.add(identifier);
			as.ifPresent(a -> a.build(args));
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
		private List<Load> loads = new ArrayList<>();
		private boolean verbatim;
		private OptionalLong timeout = OptionalLong.empty();

		private Builder() {
		}

		private Builder(AggregateOperation<K, V> operation) {
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

		public Builder<K, V> loadAll() {
			this.loads = Collections.singletonList(LOAD_ALL);
			return this;
		}

		public Builder<K, V> load(String identifier) {
			this.loads.add(Load.identifier(identifier).build());
			return this;
		}

		public Builder<K, V> loads(String... identifiers) {
			Collections.addAll(this.loads,
					Stream.of(identifiers).map(i -> Load.identifier(i).build()).toArray(Load[]::new));
			return this;
		}

		public Builder<K, V> load(Load load) {
			this.loads.add(load);
			return this;
		}

		public Builder<K, V> loads(Load... loads) {
			Collections.addAll(this.loads, loads);
			return this;
		}

		public Builder<K, V> verbatim(boolean verbatim) {
			this.verbatim = verbatim;
			return this;
		}

		public Builder<K, V> timeout(long timeout) {
			this.timeout = OptionalLong.of(timeout);
			return this;
		}

		public AggregateOptions<K, V> build() {
			return new AggregateOptions<>(this);
		}

	}

}
