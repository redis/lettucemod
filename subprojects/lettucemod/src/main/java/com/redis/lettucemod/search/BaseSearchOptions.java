package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class BaseSearchOptions<K, V> implements RediSearchArgument<K, V> {

	private boolean verbatim;
	private OptionalLong timeout = OptionalLong.empty();
	private Optional<Limit> limit = Optional.empty();
	private List<Parameter<K, V>> params = new ArrayList<>();
	private OptionalInt dialect = OptionalInt.empty();

	protected BaseSearchOptions() {
	}

	protected BaseSearchOptions(Builder<K, V, ?> builder) {
		this.verbatim = builder.verbatim;
		this.timeout = builder.timeout;
		this.limit = builder.limit;
		this.params = builder.params;
		this.dialect = builder.dialect;
	}

	public OptionalLong getTimeout() {
		return timeout;
	}

	public void setTimeout(OptionalLong timeout) {
		this.timeout = timeout;
	}

	public boolean isVerbatim() {
		return verbatim;
	}

	public void setVerbatim(boolean verbatim) {
		this.verbatim = verbatim;
	}

	public Optional<Limit> getLimit() {
		return limit;
	}

	public void setLimit(Limit limit) {
		this.limit = Optional.of(limit);
	}

	public List<Parameter<K, V>> getParams() {
		return params;
	}

	public void setParams(List<Parameter<K, V>> params) {
		this.params = params;
	}

	public OptionalInt getDialect() {
		return dialect;
	}

	public void setDialect(OptionalInt dialect) {
		this.dialect = dialect;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void build(SearchCommandArgs<K, V> args) {
		if (verbatim) {
			args.add(SearchCommandKeyword.VERBATIM);
		}
		timeout.ifPresent(t -> args.add(SearchCommandKeyword.TIMEOUT).add(t));
		limit.ifPresent(l -> l.build((SearchCommandArgs) args));
		if (!params.isEmpty()) {
			args.add(SearchCommandKeyword.PARAMS);
			args.add(params.size());
			params.forEach(p -> args.addKey(p.getName()).addValue(p.getValue()));
		}
		dialect.ifPresent(d -> args.add(SearchCommandKeyword.DIALECT).add(d));
	}

	public static class Builder<K, V, B extends Builder<K, V, B>> {

		private boolean verbatim;
		private OptionalLong timeout = OptionalLong.empty();
		private final List<Parameter<K, V>> params = new ArrayList<>();
		private Optional<Limit> limit = Optional.empty();
		private OptionalInt dialect = OptionalInt.empty();

		protected Builder() {
		}

		@SuppressWarnings("unchecked")
		public B verbatim(boolean verbatim) {
			this.verbatim = verbatim;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B timeout(long timeout) {
			this.timeout = OptionalLong.of(timeout);
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B param(K name, V value) {
			this.params.add(Parameter.of(name, value));
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B params(Parameter<K, V>... params) {
			this.params.addAll(Arrays.asList(params));
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B params(Map<K, V> map) {
			map.forEach(this::param);
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B limit(Limit limit) {
			this.limit = Optional.of(limit);
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B dialect(int version) {
			this.dialect = OptionalInt.of(version);
			return (B) this;
		}

	}
}
