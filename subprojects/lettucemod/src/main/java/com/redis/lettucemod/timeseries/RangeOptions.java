package com.redis.lettucemod.timeseries;

public class RangeOptions extends BaseRangeOptions {

	private RangeOptions(Builder builder) {
		super(builder);
	}

	public static Builder all() {
		return new Builder(START, END);
	}

	public static Builder from(long from) {
		return new Builder(from, END);
	}

	public static Builder to(long to) {
		return new Builder(START, to);
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
