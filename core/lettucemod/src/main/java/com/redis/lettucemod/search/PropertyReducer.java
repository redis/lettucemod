package com.redis.lettucemod.search;

import io.lettuce.core.internal.LettuceAssert;

public abstract class PropertyReducer extends Reducer {

	private final String name;
	protected final String property;

	protected PropertyReducer(String name, Builder<?> builder) {
		super(builder.as);
		this.name = name;
		this.property = builder.property;
	}

	public String getName() {
		return name;
	}

	public String getProperty() {
		return property;
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
		return toString(name + "(" + property + ")");
	}

}
