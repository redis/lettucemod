package com.redis.lettucemod.search;

import java.util.Optional;

import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class Reducers {

	private Reducers() {
	}

	public static class Max extends PropertyReducer {

		private Max(Builder builder) {
			super("Max", builder);
		}

		@Override
		protected void buildFunction(SearchCommandArgs<Object, Object> args) {
			args.add(SearchCommandKeyword.MAX);
			args.add(1);
			args.addProperty(property);
		}

		public static Builder property(String property) {
			return new Builder(property);
		}

		public static class Builder extends PropertyReducer.Builder<Builder> {

			public Builder(String property) {
				super(property);
			}

			public Max build() {
				return new Max(this);
			}
		}

	}

	public static class FirstValue extends PropertyReducer {

		private final Optional<By> by;

		private FirstValue(Builder builder) {
			super("FirstValue", builder);
			this.by = builder.by;
		}

		@Override
		protected void buildFunction(SearchCommandArgs<Object, Object> args) {
			args.add(SearchCommandKeyword.FIRST_VALUE);
			args.add(getNumberOfArgs());
			args.addProperty(property);
			by.ifPresent(b -> b.build(args));
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder(super.toString());
			by.ifPresent(b -> builder.append(" by=").append(by));
			return builder.toString();
		}

		private int getNumberOfArgs() {
			if (by.isPresent()) {
				if (by.get().getOrder().isPresent()) {
					return 3;
				}
				return 2;
			}
			return 1;
		}

		public static Builder property(String property) {
			return new Builder(property);
		}

		public static class Builder extends PropertyReducer.Builder<Builder> {

			private Optional<By> by = Optional.empty();

			protected Builder(String property) {
				super(property);
			}

			public Builder by(By by) {
				this.by = Optional.of(by);
				return this;
			}

			public FirstValue build() {
				return new FirstValue(this);
			}
		}

		public static class By implements RediSearchArgument<Object, Object> {

			private final String property;
			private final Optional<Order> order;

			public By(String property) {
				this.property = property;
				this.order = Optional.empty();
			}

			public By(String property, Order order) {
				this.property = property;
				this.order = Optional.of(order);
			}

			public Optional<Order> getOrder() {
				return order;
			}

			@Override
			public String toString() {
				String string = "By [property=" + property;
				if (order.isPresent()) {
					string += " " + order.get();
				}
				string += "]";
				return string;
			}

			@Override
			public void build(SearchCommandArgs<Object, Object> args) {
				args.add(SearchCommandKeyword.BY).addProperty(property);
				order.ifPresent(o -> args.add(o.getKeyword()));
			}

			public static By asc(String property) {
				return new By(property, Order.ASC);
			}

			public static By desc(String property) {
				return new By(property, Order.DESC);
			}

		}
	}

	public static class Count extends Reducer {

		private Count(Optional<String> as) {
			super(as);
		}

		@Override
		protected void buildFunction(SearchCommandArgs<Object, Object> args) {
			args.add(SearchCommandKeyword.COUNT);
			args.add(0);
		}

		@Override
		public String toString() {
			return toString("count");
		}

		public static Count create() {
			return new Count(Optional.empty());
		}

		public static Count as(String as) {
			return new Count(Optional.of(as));
		}

	}

	public static class Min extends PropertyReducer {

		private Min(Builder builder) {
			super("Min", builder);
		}

		@Override
		protected void buildFunction(SearchCommandArgs<Object, Object> args) {
			args.add(SearchCommandKeyword.MIN);
			args.add(1);
			args.addProperty(property);
		}

		public static Builder property(String property) {
			return new Builder(property);
		}

		public static class Builder extends PropertyReducer.Builder<Builder> {

			public Builder(String property) {
				super(property);
			}

			public Min build() {
				return new Min(this);
			}
		}

	}

	public static class RandomSample extends PropertyReducer {

		private final int size;

		private RandomSample(Builder builder) {
			super("RandomSample", builder);
			this.size = builder.size;
		}

		@Override
		public String toString() {
			return super.toString() + " size=" + size;
		}

		@Override
		protected void buildFunction(SearchCommandArgs<Object, Object> args) {
			args.add(SearchCommandKeyword.RANDOM_SAMPLE);
			args.add(2);
			args.addProperty(property);
			args.add(size);
		}

		public static SizeBuilder property(String property) {
			return new SizeBuilder(property);
		}

		public static class SizeBuilder {

			private final String property;

			public SizeBuilder(String property) {
				this.property = property;
			}

			public Builder size(int size) {
				return new Builder(property, size);
			}

		}

		public static class Builder extends PropertyReducer.Builder<Builder> {

			private final int size;

			public Builder(String property, int size) {
				super(property);
				this.size = size;
			}

			public RandomSample build() {
				return new RandomSample(this);
			}

		}

	}

	public static class Avg extends PropertyReducer {

		private Avg(Builder builder) {
			super("Avg", builder);
		}

		@Override
		protected void buildFunction(SearchCommandArgs<Object, Object> args) {
			args.add(SearchCommandKeyword.AVG);
			args.add(1);
			args.addProperty(property);
		}

		public static Builder property(String property) {
			return new Builder(property);
		}

		public static class Builder extends PropertyReducer.Builder<Builder> {

			public Builder(String property) {
				super(property);
			}

			public Avg build() {
				return new Avg(this);
			}
		}

	}

	public static class ToList extends PropertyReducer {

		private ToList(Builder builder) {
			super("ToList", builder);
		}

		@Override
		protected void buildFunction(SearchCommandArgs<Object, Object> args) {
			args.add(SearchCommandKeyword.TOLIST);
			args.add(1);
			args.addProperty(property);
		}

		public static Builder property(String property) {
			return new Builder(property);
		}

		public static class Builder extends PropertyReducer.Builder<Builder> {

			public Builder(String property) {
				super(property);
			}

			public ToList build() {
				return new ToList(this);
			}
		}
	}

	public static class Sum extends PropertyReducer {

		private Sum(Builder builder) {
			super("Sum", builder);
		}

		@Override
		protected void buildFunction(SearchCommandArgs<Object, Object> args) {
			args.add(SearchCommandKeyword.SUM);
			args.add(1);
			args.addProperty(property);
		}

		public static Builder property(String property) {
			return new Builder(property);
		}

		public static class Builder extends PropertyReducer.Builder<Builder> {

			public Builder(String property) {
				super(property);
			}

			public Sum build() {
				return new Sum(this);
			}
		}
	}

	public static class StdDev extends PropertyReducer {

		private StdDev(Builder builder) {
			super("StdDev", builder);
		}

		@Override
		protected void buildFunction(SearchCommandArgs<Object, Object> args) {
			args.add(SearchCommandKeyword.STDDEV);
			args.add(1);
			args.addProperty(property);
		}

		public static Builder property(String property) {
			return new Builder(property);
		}

		public static class Builder extends PropertyReducer.Builder<Builder> {

			public Builder(String property) {
				super(property);
			}

			public StdDev build() {
				return new StdDev(this);
			}
		}

	}

	public static class CountDistinctish extends PropertyReducer {

		private CountDistinctish(Builder builder) {
			super("CountDistinctish", builder);
		}

		@Override
		protected void buildFunction(SearchCommandArgs<Object, Object> args) {
			args.add(SearchCommandKeyword.COUNT_DISTINCTISH);
			args.add(1);
			args.addProperty(property);
		}

		public static Builder property(String property) {
			return new Builder(property);
		}

		public static class Builder extends PropertyReducer.Builder<Builder> {

			public Builder(String property) {
				super(property);
			}

			public CountDistinctish build() {
				return new CountDistinctish(this);
			}
		}

	}

	public static class CountDistinct extends PropertyReducer {

		public CountDistinct(Builder builder) {
			super("CountDistinct", builder);
		}

		@Override
		protected void buildFunction(SearchCommandArgs<Object, Object> args) {
			args.add(SearchCommandKeyword.COUNT_DISTINCT);
			args.add(1);
			args.addProperty(property);
		}

		public static Builder property(String property) {
			return new Builder(property);
		}

		public static class Builder extends PropertyReducer.Builder<Builder> {

			public Builder(String property) {
				super(property);
			}

			public CountDistinct build() {
				return new CountDistinct(this);
			}

		}

	}

	public static class Quantile extends PropertyReducer {

		private final double quantileValue;

		private Quantile(Builder builder) {
			super("Quantile", builder);
			this.quantileValue = builder.quantile;
		}

		@Override
		public String toString() {
			return super.toString() + " quantile=" + quantileValue;
		}

		@Override
		protected void buildFunction(SearchCommandArgs<Object, Object> args) {
			args.add(SearchCommandKeyword.QUANTILE);
			args.add(2);
			args.addProperty(property);
			args.add(quantileValue);
		}

		public static QuantileBuilder property(String property) {
			return new QuantileBuilder(property);
		}

		public static class QuantileBuilder {

			private final String property;

			public QuantileBuilder(String property) {
				this.property = property;
			}

			public Builder quantile(double quantile) {
				return new Builder(property, quantile);
			}

		}

		public static class Builder extends PropertyReducer.Builder<Builder> {

			private final double quantile;

			protected Builder(String property, double quantile) {
				super(property);
				this.quantile = quantile;
			}

			public Quantile build() {
				return new Quantile(this);
			}

		}

	}

}
