package com.redis.lettucemod.search;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class BaseSearchOptions<K, V> implements RediSearchArgument<K, V> {

	private boolean verbatim;
	private Optional<Duration> timeout = Optional.empty();
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

	public Optional<Duration> getTimeout() {
		return timeout;
	}

	public void setTimeout(Optional<Duration> timeout) {
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

	@Override
	public void build(SearchCommandArgs<K, V> args) {
		if (verbatim) {
			args.add(SearchCommandKeyword.VERBATIM);
		}
		timeout.ifPresent(t -> args.add(SearchCommandKeyword.TIMEOUT).add(t.toMillis()));
		limit.ifPresent(l -> l.build(args));
		if (!params.isEmpty()) {
			args.add(SearchCommandKeyword.PARAMS);
			args.add(params.size() * 2l);
			params.forEach(p -> args.addKey(p.getName()).addValue(p.getValue()));
		}
		dialect.ifPresent(d -> args.add(SearchCommandKeyword.DIALECT).add(d));
	}

	public static class Builder<K, V, B extends Builder<K, V, B>> {

		private boolean verbatim;
		private Optional<Duration> timeout = Optional.empty();
		private final List<Parameter<K, V>> params = new ArrayList<>();
		private Optional<Limit> limit = Optional.empty();
		private OptionalInt dialect = OptionalInt.empty();

		protected Builder() {
		}

		public B verbatim() {
			return verbatim(true);
		}

		@SuppressWarnings("unchecked")
		public B verbatim(boolean verbatim) {
			this.verbatim = verbatim;
			return (B) this;
		}

		public B timeout(long millis) {
			return timeout(Duration.ofMillis(millis));
		}

		@SuppressWarnings("unchecked")
		public B timeout(Duration timeout) {
			this.timeout = Optional.of(timeout);
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

		public B limit(long offset, long num) {
			return limit(Limit.offset(offset).num(num));
		}

		@SuppressWarnings("unchecked")
		public B dialect(int version) {
			this.dialect = OptionalInt.of(version);
			return (B) this;
		}

	}
}
