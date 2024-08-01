package com.redis.lettucemod.timeseries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class Filters<V> implements CompositeArgument {

	private List<V> expressions = new ArrayList<>();

	public Filters() {
	}

	public Filters(List<V> expressions) {
		this.expressions = expressions;
	}

	public List<V> getExpressions() {
		return expressions;
	}

	public void setExpressions(List<V> expressions) {
		this.expressions = expressions;
	}

	@SuppressWarnings({ "hiding", "unchecked" })
	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		args.add(TimeSeriesCommandKeyword.FILTER);
		expressions.forEach(f -> args.addValue((V) f));
	}

	@SuppressWarnings("unchecked")
	public static <K> Filters<K> of(K... expressions) {
		return new Filters<>(Arrays.asList(expressions));
	}

}
