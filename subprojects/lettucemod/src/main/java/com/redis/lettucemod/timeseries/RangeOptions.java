package com.redis.lettucemod.timeseries;

import io.lettuce.core.protocol.CommandArgs;

public class RangeOptions extends BaseRangeOptions {

	private RangeOptions(Builder builder) {
		super(builder);
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		buildFilterByTimestamp(args);
		buildFilterByValue(args);
		buildCount(args);
		buildAggregation(args);
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder extends BaseRangeOptions.Builder<Builder> {

		public RangeOptions build() {
			return new RangeOptions(this);
		}

	}

}
