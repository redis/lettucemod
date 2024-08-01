package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public class Filter<V> implements AggregateOperation {

	private final V expression;

	public Filter(V expression) {
		this.expression = expression;
	}

	@Override
	public Type getType() {
		return Type.FILTER;
	}

	public V getExpression() {
		return expression;
	}

	@Override
	public String toString() {
		return "FILTER " + expression;
	}

	public static <V> Filter<V> expression(V expression) {
		return new Filter<>(expression);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void build(SearchCommandArgs args) {
		args.add(SearchCommandKeyword.FILTER);
		args.addValue(expression);
	}

}
