package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class Filter<V> implements AggregateOperation<Object, V> {

	private final V expression;

	public Filter(V expression) {
		this.expression = expression;
	}

	@Override
	public String toString() {
		return "FILTER " + expression;
	}

	public static <V> Filter<V> expression(V expression) {
		return new Filter<>(expression);
	}

	@Override
	public void build(SearchCommandArgs<Object, V> args) {
		args.add(SearchCommandKeyword.FILTER);
		args.addValue(expression);
	}

}
