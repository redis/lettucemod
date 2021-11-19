package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public class Filter<V> implements AggregateOperation {

	private final V expression;

	public Filter(V expression) {
		this.expression = expression;
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
