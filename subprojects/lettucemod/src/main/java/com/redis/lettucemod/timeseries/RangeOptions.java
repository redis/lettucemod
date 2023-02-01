package com.redis.lettucemod.timeseries;

import io.lettuce.core.protocol.CommandArgs;

public class RangeOptions extends AbstractRangeOptions {

	private RangeOptions(Builder builder) {
		super(builder);
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		buildLatest(args);
		buildFilterByTimestamp(args);
		buildFilterByValue(args);
		buildCount(args);
		buildAggregation(args);
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder extends AbstractRangeOptions.Builder<Builder> {

		public RangeOptions build() {
			return new RangeOptions(this);
		}

	}

}
