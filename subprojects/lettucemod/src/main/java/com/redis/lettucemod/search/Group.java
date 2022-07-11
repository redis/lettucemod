package com.redis.lettucemod.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.redis.lettucemod.protocol.SearchCommandKeyword;
import com.redis.lettucemod.search.Reducers.Avg;
import com.redis.lettucemod.search.Reducers.Count;
import com.redis.lettucemod.search.Reducers.CountDistinct;
import com.redis.lettucemod.search.Reducers.CountDistinctish;
import com.redis.lettucemod.search.Reducers.FirstValue;
import com.redis.lettucemod.search.Reducers.Max;
import com.redis.lettucemod.search.Reducers.Min;
import com.redis.lettucemod.search.Reducers.Quantile;
import com.redis.lettucemod.search.Reducers.RandomSample;
import com.redis.lettucemod.search.Reducers.StdDev;
import com.redis.lettucemod.search.Reducers.Sum;
import com.redis.lettucemod.search.Reducers.ToList;

import io.lettuce.core.internal.LettuceAssert;

public class Group implements AggregateOperation<Object, Object> {

	private final String[] properties;
	private final Reducer[] reducers;

	public Group(String[] properties, Reducer[] reducers) {
		LettuceAssert.notNull(properties, "Properties must not be null");
		LettuceAssert.noNullElements(properties, "Property elements must not be null");
		LettuceAssert.notEmpty(reducers, "Group must have at least one reducer");
		LettuceAssert.noNullElements(reducers, "Reducer elements must not be null");
		this.properties = properties;
		this.reducers = reducers;
	}

	@Override
	public void build(SearchCommandArgs<Object, Object> args) {
		args.add(SearchCommandKeyword.GROUPBY);
		args.add(properties.length);
		for (String property : properties) {
			args.addProperty(property);
		}
		for (Reducer reducer : reducers) {
			reducer.build(args);
		}
	}

	@Override
	public String toString() {
		return "GROUP [properties=" + Arrays.toString(properties) + ", reducers=" + Arrays.toString(reducers) + "]";
	}

	public static Builder by(String... properties) {
		return new Builder(properties);
	}

	public static class Builder {

		private final List<String> properties = new ArrayList<>();
		private final List<Reducer> reducers = new ArrayList<>();

		public Builder(String... properties) {
			Collections.addAll(this.properties, properties);
		}

		public Builder property(String property) {
			return new Builder(property);
		}

		public Builder avg(Avg avg) {
			return reducer(avg);
		}

		public Builder count(Count count) {
			return reducer(count);
		}

		public Builder countDistinct(CountDistinct countDistinct) {
			return reducer(countDistinct);
		}

		public Builder countDistinctish(CountDistinctish countDistinctish) {
			return reducer(countDistinctish);
		}

		public Builder firstValue(FirstValue firstValue) {
			return reducer(firstValue);
		}

		public Builder min(Min min) {
			return reducer(min);
		}

		public Builder max(Max max) {
			return reducer(max);
		}

		public Builder quantile(Quantile quantile) {
			return reducer(quantile);
		}

		public Builder randomSample(RandomSample randomSample) {
			return reducer(randomSample);
		}

		public Builder stdDev(StdDev stdDev) {
			return reducer(stdDev);
		}

		public Builder sum(Sum sum) {
			return reducer(sum);
		}

		public Builder toList(ToList toList) {
			return reducer(toList);
		}

		public Builder reducer(Reducer reducer) {
			return reducers(reducer);
		}

		public Builder reducers(Reducer... reducers) {
			Collections.addAll(this.reducers, reducers);
			return this;
		}

		public Group build() {
			return new Group(properties.toArray(new String[0]), reducers.toArray(new Reducer[0]));
		}

	}

}
