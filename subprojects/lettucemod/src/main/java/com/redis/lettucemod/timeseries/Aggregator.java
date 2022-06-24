package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public enum Aggregator implements CompositeArgument {

	AVG, SUM, MIN, MAX, RANGE, COUNT, FIRST, LAST, STD_P("STD.P"), STD_S("STD.S"), VAR_P("VAR.P"), VAR_S("VAR.S"), TWA;

	private final String name;

	Aggregator(String name) {
		this.name = name;
	}

	Aggregator() {
		this.name = this.name();
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		args.add(TimeSeriesCommandKeyword.AGGREGATION);
		args.add(name);
	}

}