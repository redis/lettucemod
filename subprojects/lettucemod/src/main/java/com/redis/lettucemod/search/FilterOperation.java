package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public class FilterOperation<V> implements AggregateOperation {

	private final V expression;

	public FilterOperation(V expression) {
		this.expression = expression;
	}

	public static <V> FilterOperation<V> expression(V expression) {
		return new FilterOperation<>(expression);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void build(SearchCommandArgs args) {
		args.add(SearchCommandKeyword.FILTER);
		args.addValue(expression);
	}

}
