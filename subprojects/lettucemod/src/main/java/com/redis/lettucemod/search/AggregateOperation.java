package com.redis.lettucemod.search;

import com.redis.lettucemod.search.Limit.NumBuilder;
import com.redis.lettucemod.search.Sort.Property;

public interface AggregateOperation<K, V> extends RediSearchArgument<K, V> {

	public enum Type {
		APPLY, FILTER, GROUP, LIMIT, SORT
	}

	Type getType();

	static <K, V> Apply.Builder<K, V> apply(V expression) {
		return Apply.expression(expression);
	}

	static <V> Filter<V> filter(V expression) {
		return Filter.expression(expression);
	}

	static Group.Builder groupBy(String... properties) {
		return Group.by(properties);
	}

	static NumBuilder limit(long offset) {
		return Limit.offset(offset);
	}

	static Sort.Builder sort(Property... properties) {
		return Sort.by(properties);
	}

}