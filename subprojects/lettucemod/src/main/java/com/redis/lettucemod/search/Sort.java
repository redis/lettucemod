package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

import io.lettuce.core.internal.LettuceAssert;

@SuppressWarnings("rawtypes")
public class Sort implements AggregateOperation {

	private final List<Property> properties;
	private final Optional<Max> max;

	private Sort(Builder builder) {
		this.properties = builder.properties;
		this.max = builder.max;
	}

	@Override
	public void build(SearchCommandArgs args) {
		args.add(SearchCommandKeyword.SORTBY);
		args.add((long) properties.size() * 2);
		for (Property property : properties) {
			property.build(args);
		}
		max.ifPresent(m -> m.build(args));
	}

	public static Builder by(Property... properties) {
		return new Builder(properties);
	}

	public static class Builder {

		private final List<Property> properties = new ArrayList<>();
		private Optional<Max> max = Optional.empty();

		public Builder(Property... properties) {
			Collections.addAll(this.properties, properties);
		}

		public Builder by(Property property) {
			return by(property);
		}

		public Builder max(long max) {
			this.max = Optional.of(new Max(max));
			return this;
		}

		public Sort build() {
			return new Sort(this);
		}

	}

	public static class Property implements RediSearchArgument {

		private final String name;
		private final Order order;

		private Property(String name, Order order) {
			LettuceAssert.notNull(name, "Name is required");
			LettuceAssert.notNull(order, "Order is required");
			this.name = name;
			this.order = order;
		}

		public static Property of(String name, Order order) {
			return new Property(name, order);
		}

		public static Property asc(String name) {
			return new Property(name, Order.ASC);
		}

		public static Property desc(String name) {
			return new Property(name, Order.DESC);
		}

		@Override
		public void build(SearchCommandArgs args) {
			args.addProperty(name);
			order.build(args);
		}

	}
}
