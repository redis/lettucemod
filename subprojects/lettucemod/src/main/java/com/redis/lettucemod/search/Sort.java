package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

import io.lettuce.core.internal.LettuceAssert;

@SuppressWarnings("rawtypes")
public class Sort implements AggregateOperation {

	private final Property[] properties;
	private final Long max;

	public Sort(Property[] properties, Long max) {
		LettuceAssert.notEmpty(properties, "At least one property is required");
		LettuceAssert.noNullElements(properties, "Properties must not be null");
		this.properties = properties;
		this.max = max;
	}

	@Override
	public void build(SearchCommandArgs args) {
		args.add(SearchCommandKeyword.SORTBY);
		args.add((long) properties.length * 2);
		for (Property property : properties) {
			property.build(args);
		}
		if (max != null) {
			args.add(SearchCommandKeyword.MAX);
			args.add(max);
		}
	}

	public static SortBuilder by(Property... properties) {
		return new SortBuilder(properties);
	}

	public static class SortBuilder {

		private final List<Property> properties = new ArrayList<>();
		private Long max;

		public SortBuilder(Property... properties) {
			Collections.addAll(this.properties, properties);
		}

		public SortBuilder by(Property property) {
			return by(property);
		}

		public SortBuilder max(long max) {
			this.max = max;
			return this;
		}

		public Sort build() {
			return new Sort(properties.toArray(new Property[0]), max);
		}

	}

	public static class Property implements RediSearchArgument {

		private final String name;
		private final Order order;

		public Property(String name, Order order) {
			LettuceAssert.notNull(name, "Name is required");
			LettuceAssert.notNull(order, "Order is required");
			this.name = name;
			this.order = order;
		}

		@Override
		public void build(SearchCommandArgs args) {
			args.addProperty(name);
			args.add(order == Order.ASC ? SearchCommandKeyword.ASC : SearchCommandKeyword.DESC);
		}

		public static PropertyBuilder name(String name) {
			return new PropertyBuilder(name);
		}

		public static class PropertyBuilder {

			private final String name;

			public PropertyBuilder(String name) {
				this.name = name;
			}

			public Property order(Order order) {
				return new Property(name, order);
			}
		}

	}
}
