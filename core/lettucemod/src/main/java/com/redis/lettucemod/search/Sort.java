package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

import io.lettuce.core.internal.LettuceAssert;
import lombok.ToString;

@SuppressWarnings("rawtypes")
@ToString
public class Sort implements AggregateOperation {

	private List<Property> properties;
	private Optional<Max> max;

	private Sort(Builder builder) {
		this.properties = builder.properties;
		this.max = builder.max;
	}

	@Override
	public Type getType() {
		return Type.SORT;
	}

	public List<Property> getProperties() {
		return properties;
	}

	public Optional<Max> getMax() {
		return max;
	}

	public void setProperties(List<Property> properties) {
		this.properties = properties;
	}

	public void setMax(Optional<Max> max) {
		this.max = max;
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

	public static Builder asc(String name) {
		return by(Property.asc(name));
	}

	public static Builder desc(String name) {
		return by(Property.desc(name));
	}

	public static class Builder {

		private final List<Property> properties = new ArrayList<>();
		private Optional<Max> max = Optional.empty();

		public Builder(Property... properties) {
			Collections.addAll(this.properties, properties);
		}

		public Builder by(Property... properties) {
			this.properties.addAll(Arrays.asList(properties));
			return this;
		}

		public Builder asc(String name) {
			return by(Property.asc(name));
		}

		public Builder desc(String name) {
			return by(Property.desc(name));
		}

		public Builder max(long max) {
			this.max = Optional.of(new Max(max));
			return this;
		}

		public Sort build() {
			return new Sort(this);
		}

	}

	@ToString
	public static class Property implements RediSearchArgument {

		private String name;
		private Order order;

		private Property(String name, Order order) {
			LettuceAssert.notNull(name, "Name is required");
			LettuceAssert.notNull(order, "Order is required");
			this.name = name;
			this.order = order;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Order getOrder() {
			return order;
		}

		public void setOrder(Order order) {
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
			args.add(order.getKeyword());
		}

	}
}
