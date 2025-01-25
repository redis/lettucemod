package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

import lombok.ToString;

@ToString
public class Apply<K, V> implements AggregateOperation<K, V> {

	private final V expression;
	private final K as;

	public Apply(V expression, K as) {
		this.expression = expression;
		this.as = as;
	}

	@Override
	public Type getType() {
		return Type.APPLY;
	}

	public V getExpression() {
		return expression;
	}

	public K getAs() {
		return as;
	}

	@Override
	public void build(SearchCommandArgs<K, V> args) {
		args.add(SearchCommandKeyword.APPLY);
		args.addValue(expression);
		args.add(SearchCommandKeyword.AS).addKey(as);
	}

	public static <K, V> Builder<K, V> expression(V expression) {
		return new Builder<>(expression);
	}

	public static class Builder<K, V> {

		private final V expression;

		public Builder(V expression) {
			this.expression = expression;
		}

		public Apply<K, V> as(K as) {
			return new Apply<>(expression, as);
		}
	}

}
