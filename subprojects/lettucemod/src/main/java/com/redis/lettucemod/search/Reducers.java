package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public class Reducers {

	private Reducers() {
	}

	public static class Max extends PropertyReducer {

		public Max(String as, String property) {
			super(as, property);
		}

		@Override
		protected void buildFunction(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.MAX);
			args.add(1);
			args.addProperty(property);
		}

		public static MaxBuilder property(String property) {
			return new MaxBuilder(property);
		}

		public static class MaxBuilder extends PropertyReducerBuilder<MaxBuilder> {

			public MaxBuilder(String property) {
				super(property);
			}

			public Max build() {
				return new Max(as, property);
			}
		}

	}

	public static class FirstValue extends PropertyReducer {

		private final By by;

		public FirstValue(String as, String property, By by) {
			super(as, property);
			this.by = by;
		}

		@Override
		protected void buildFunction(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.FIRST_VALUE);
			args.add(getNumberOfArgs());
			args.addProperty(property);
			if (by != null) {
				args.add(SearchCommandKeyword.BY);
				args.addProperty(property);
				if (by.getOrder() != null) {
					args.add(by.getOrder() == Order.ASC ? SearchCommandKeyword.ASC : SearchCommandKeyword.DESC);
				}
			}
		}

		private int getNumberOfArgs() {
			int nargs = 1;
			if (by != null) {
				nargs += by.getOrder() == null ? 2 : 3;
			}
			return nargs;
		}

		public static FirstValueBuilder property(String property) {
			return new FirstValueBuilder(property);
		}

		public static class FirstValueBuilder extends PropertyReducerBuilder<FirstValueBuilder> {

			private By by;

			protected FirstValueBuilder(String property) {
				super(property);
			}

			public FirstValueBuilder by(By by) {
				this.by = by;
				return this;
			}

			public FirstValue build() {
				return new FirstValue(as, property, by);
			}
		}

		public static class By {

			private String property;
			private Order order;

			public By(String property, Order order) {
				this.property = property;
				this.order = order;
			}

			public String getProperty() {
				return property;
			}

			public void setProperty(String property) {
				this.property = property;
			}

			public Order getOrder() {
				return order;
			}

			public void setOrder(Order order) {
				this.order = order;
			}

			public static ByBuilder property(String property) {
				return new ByBuilder(property);
			}

			public static class ByBuilder {

				private final String property;
				private Order order;

				public ByBuilder(String property) {
					this.property = property;
				}

				public ByBuilder order(Order order) {
					this.order = order;
					return this;
				}

				public By build() {
					return new By(property, order);
				}

			}

		}
	}

	public static class Count extends Reducer {

		public Count(String as) {
			super(as);
		}

		@Override
		protected void buildFunction(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.COUNT);
			args.add(0);
		}

		public static Count create() {
			return new Count(null);
		}

		public static Count as(String as) {
			return new Count(as);
		}

	}

	public static class Min extends PropertyReducer {

		public Min(String as, String property) {
			super(as, property);
		}

		@Override
		protected void buildFunction(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.MIN);
			args.add(1);
			args.addProperty(property);
		}

		public static MinBuilder property(String property) {
			return new MinBuilder(property);
		}

		public static class MinBuilder extends PropertyReducerBuilder<MinBuilder> {

			public MinBuilder(String property) {
				super(property);
			}

			public Min build() {
				return new Min(as, property);
			}
		}

	}

	public static class RandomSample extends PropertyReducer {

		private final int size;

		public RandomSample(String as, String property, int size) {
			super(as, property);
			this.size = size;
		}

		@Override
		protected void buildFunction(SearchCommandArgs args) {
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

		public static class Builder extends PropertyReducerBuilder<Builder> {

			private final int size;

			public Builder(String property, int size) {
				super(property);
				this.size = size;
			}

			public RandomSample build() {
				return new RandomSample(as, property, size);
			}

		}

	}

	public static class Avg extends PropertyReducer {

		public Avg(String as, String property) {
			super(as, property);
		}

		@Override
		protected void buildFunction(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.AVG);
			args.add(1);
			args.addProperty(property);
		}

		public static AvgBuilder property(String property) {
			return new AvgBuilder(property);
		}

		public static class AvgBuilder extends PropertyReducer.PropertyReducerBuilder<AvgBuilder> {

			public AvgBuilder(String property) {
				super(property);
			}

			public Avg build() {
				return new Avg(as, property);
			}
		}

	}

	public static class ToList extends PropertyReducer {

		public ToList(String as, String property) {
			super(as, property);
		}

		@Override
		protected void buildFunction(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.TOLIST);
			args.add(1);
			args.addProperty(property);
		}

		public static ToListBuilder property(String property) {
			return new ToListBuilder(property);
		}

		public static class ToListBuilder extends PropertyReducerBuilder<ToListBuilder> {

			public ToListBuilder(String property) {
				super(property);
			}

			public ToList build() {
				return new ToList(as, property);
			}
		}
	}

	public static class Sum extends PropertyReducer {

		public Sum(String as, String property) {
			super(as, property);
		}

		@Override
		protected void buildFunction(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.SUM);
			args.add(1);
			args.addProperty(property);
		}

		public static SumBuilder property(String property) {
			return new SumBuilder(property);
		}

		public static class SumBuilder extends PropertyReducerBuilder<SumBuilder> {

			public SumBuilder(String property) {
				super(property);
			}

			public Sum build() {
				return new Sum(as, property);
			}
		}
	}

	public static class StdDev extends PropertyReducer {

		public StdDev(String as, String property) {
			super(as, property);
		}

		@Override
		protected void buildFunction(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.STDDEV);
			args.add(1);
			args.addProperty(property);
		}

		public static StdDevBuilder property(String property) {
			return new StdDevBuilder(property);
		}

		public static class StdDevBuilder extends PropertyReducerBuilder<StdDevBuilder> {

			public StdDevBuilder(String property) {
				super(property);
			}

			public StdDev build() {
				return new StdDev(as, property);
			}
		}

	}

	public static class CountDistinctish extends PropertyReducer {

		private CountDistinctish(String as, String property) {
			super(as, property);
		}

		@Override
		protected void buildFunction(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.COUNT_DISTINCTISH);
			args.add(1);
			args.addProperty(property);
		}

		public static CountDistinctishBuilder property(String property) {
			return new CountDistinctishBuilder(property);
		}

		public static class CountDistinctishBuilder extends PropertyReducerBuilder<CountDistinctishBuilder> {

			public CountDistinctishBuilder(String property) {
				super(property);
			}

			public CountDistinctish build() {
				return new CountDistinctish(as, property);
			}
		}

	}

	public static class CountDistinct extends PropertyReducer {

		public CountDistinct(String as, String property) {
			super(as, property);
		}

		@Override
		protected void buildFunction(SearchCommandArgs args) {
			args.add(SearchCommandKeyword.COUNT_DISTINCT);
			args.add(1);
			args.addProperty(property);
		}

		public static CountDistinctBuilder property(String property) {
			return new CountDistinctBuilder(property);
		}

		public static class CountDistinctBuilder extends PropertyReducerBuilder<CountDistinctBuilder> {

			public CountDistinctBuilder(String property) {
				super(property);
			}

			public CountDistinct build() {
				return new CountDistinct(as, property);
			}

		}

	}

	public static class Quantile extends PropertyReducer {

		private final double quantileValue;

		public Quantile(String as, String property, double quantile) {
			super(as, property);
			this.quantileValue = quantile;
		}

		@Override
		protected void buildFunction(SearchCommandArgs args) {
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

		public static class Builder extends PropertyReducerBuilder<Builder> {

			private final double quantile;

			protected Builder(String property, double quantile) {
				super(property);
				this.quantile = quantile;
			}

			public Quantile build() {
				return new Quantile(as, property, quantile);
			}

		}

	}

}
