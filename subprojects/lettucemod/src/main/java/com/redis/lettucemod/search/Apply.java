package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class Apply<K, V> implements AggregateOperation<K, V> {

	private final V expression;
	private final As as;

	public Apply(V expression, As as) {
		this.expression = expression;
		this.as = as;
	}

	@Override
	public void build(SearchCommandArgs<K, V> args) {
		args.add(SearchCommandKeyword.APPLY);
		args.addValue(expression);
		as.build(args);
	}

	@Override
	public String toString() {
		return "APPLY " + expression + " " + as;
	}

	public static <K, V> Builder<K, V> expression(V expression) {
		return new Builder<>(expression);
	}

	public static class Builder<K, V> {

		private final V expression;

		public Builder(V expression) {
			this.expression = expression;
		}

		public Apply<K, V> as(String as) {
			return new Apply<>(expression, As.of(as));
		}
	}

}
