package com.redis.lettucemod.timeseries;

public class RangeOptions extends BaseRangeOptions {

	private RangeOptions(Builder builder) {
		super(builder);
	}

	public static Builder from(long from) {
		return new Builder(from, Long.MAX_VALUE);
	}

	public static Builder to(long to) {
		return new Builder(Long.MIN_VALUE, to);
	}

	public static Builder range(long from, long to) {
		return new Builder(from, to);
	}

	public static class Builder extends BaseRangeOptions.Builder<Builder> {

		public Builder(long from, long to) {
			super(from, to);
		}

		public RangeOptions build() {
			return new RangeOptions(this);
		}

	}

}
