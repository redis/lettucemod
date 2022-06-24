package com.redis.lettucemod.timeseries;

public class Timestamp {

	public static final Timestamp UNBOUNDED = new Timestamp(0);

	private final long value;

	public Timestamp(long value) {
		this.value = value;
	}

	public static Timestamp of(long value) {
		return new Timestamp(value);
	}

	public static Timestamp unbounded() {
		return UNBOUNDED;
	}

	public boolean isUnbounded() {
		return this == UNBOUNDED;
	}

	public long getValue() {
		return value;
	}

}
