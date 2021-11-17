package com.redis.lettucemod.timeseries;

public class Sample {

	private long timestamp;
	private double value;

	public Sample() {
	}

	public Sample(long timestamp, double value) {
		this.timestamp = timestamp;
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public static Sample of(long timestamp, double value) {
		return new Sample(timestamp, value);
	}

}