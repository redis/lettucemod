package com.redis.lettucemod.timeseries;

public class RangeOptions extends AbstractRangeOptions {

	private RangeOptions(Builder builder) {
		super(builder);
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
