package com.redis.lettucemod.search;

import com.redis.lettucemod.RedisModulesUtils;

import io.lettuce.core.internal.LettuceAssert;

public abstract class PropertyReducer extends Reducer {

	protected final String property;

	protected PropertyReducer(Builder<?> builder) {
		super(builder.as);
		this.property = builder.property;
	}

	public static class Builder<B extends Builder<B>> extends Reducer.Builder<B> {

		protected final String property;

		protected Builder(String property) {
			LettuceAssert.notNull(property, "Property is required");
			this.property = property;
		}
	}

	@Override
	public String toString() {
		return toString(RedisModulesUtils.getShortName(getClass()) + "(" + property + ")");
	}

}
