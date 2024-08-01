package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class AggregateOptions<K, V> extends BaseSearchOptions<K, V> {

	private static final Load LOAD_ALL = Load.identifier("*").build();

	private List<AggregateOperation<K, V>> operations = new ArrayList<>();
	private List<Load> loads = new ArrayList<>();

	public void setOperations(List<AggregateOperation<K, V>> operations) {
		this.operations = operations;
	}

	public void setLoads(List<Load> loads) {
		this.loads = loads;
	}

	public AggregateOptions() {
	}

	private AggregateOptions(Builder<K, V> builder) {
		super(builder);
		this.operations = builder.operations;
		this.loads = builder.loads;
	}

	public List<AggregateOperation<K, V>> getOperations() {
		return operations;
	}

	public List<Load> getLoads() {
		return loads;
	}

	@Override
	public void build(SearchCommandArgs<K, V> args) {
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
	}

	@Override
	public String toString() {
		return "AggregateOptions [operations=" + operations + ", loads=" + loads + ", getTimeout()=" + getTimeout()
				+ ", isVerbatim()=" + isVerbatim() + ", getLimit()=" + getLimit() + ", getParams()=" + getParams()
				+ ", getDialect()=" + getDialect() + "]";
	}

	@SuppressWarnings("rawtypes")
	public static class Load implements RediSearchArgument {

		private final String identifier;
		private final Optional<String> as;

		private Load(Builder builder) {
			this.identifier = builder.identifier;
			this.as = builder.as;
		}

		public int getNargs() {
			int nargs = 1;
			if (as.isPresent()) {
				nargs += 2;
			}
			return nargs;
		}

		public static Builder identifier(String identifier) {
			return new Builder(identifier);
		}

		public static class Builder {

			private final String identifier;
			private Optional<String> as = Optional.empty();

			public Builder(String identifier) {
				this.identifier = identifier;
			}

			public Builder as(String field) {
				as = Optional.of(field);
				return this;
			}

			public Load build() {
				return new Load(this);
			}
		}

		@Override
		public void build(SearchCommandArgs args) {
			args.add(identifier);
			as.ifPresent(a -> args.add(SearchCommandKeyword.AS).add(a));
		}

	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <K, V> Builder<K, V> operation(AggregateOperation operation) {
		return new Builder<>(operation);
	}

	public static class Builder<K, V> extends BaseSearchOptions.Builder<K, V, Builder<K, V>> {

		private final List<AggregateOperation<K, V>> operations = new ArrayList<>();
		private List<Load> loads = new ArrayList<>();

		private Builder() {
		}

		private Builder(AggregateOperation<K, V> operation) {
			operations.add(operation);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public Builder<K, V> operation(AggregateOperation operation) {
			this.operations.add(operation);
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

		public AggregateOptions<K, V> build() {
			return new AggregateOptions<>(this);
		}

	}

}
