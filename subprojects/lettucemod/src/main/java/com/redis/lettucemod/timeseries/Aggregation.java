package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.CommandArgs;

public class Aggregation implements CompositeArgument {

	public enum Type {

		AVG, SUM, MIN, MAX, RANGE, COUNT, FIRST, LAST, STD_P("STD.P"), STD_S("STD.S"), VAR_P("VAR.P"), VAR_S("VAR.S");

		private final String name;

		public String getName() {
			return name;
		}

		Type(String name) {
			this.name = name;
		}

		Type() {
			this.name = this.name();
		}

	}

	private final Type type;
	private final long timeBucket;

	public Aggregation(Type type, long timeBucket) {
		LettuceAssert.notNull(type, "Aggregation type is required");
		LettuceAssert.isTrue(timeBucket > 0, "A time bucket is required");
		this.type = type;
		this.timeBucket = timeBucket;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		args.add(TimeSeriesCommandKeyword.AGGREGATION);
		args.add(type.getName());
		args.add(timeBucket);
	}

	public static Aggregation of(Type type, long timeBucket) {
		return new Aggregation(type, timeBucket);
	}

}
