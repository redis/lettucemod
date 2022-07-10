package com.redis.lettucemod.timeseries;

import java.util.Objects;

public class Sample {

	public static final long AUTO_TIMESTAMP = 0;

	private long timestamp;
	private double value;

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

	public static Sample of(double value) {
		return of(AUTO_TIMESTAMP, value);
	}

	public static Sample of(long timestamp, double value) {
		Sample sample = new Sample();
		sample.timestamp = timestamp;
		sample.value = value;
		return sample;
	}

	public static Builder value(double value) {
		return new Builder(value);
	}

	public static class Builder {

		private final double value;

		public Builder(double value) {
			this.value = value;
		}

		public Sample timestamp(long timestamp) {
			Sample sample = new Sample();
			sample.timestamp = timestamp;
			sample.value = value;
			return sample;
		}

		public Sample build() {
			Sample sample = new Sample();
			sample.timestamp = AUTO_TIMESTAMP;
			sample.value = value;
			return sample;
		}

	}

	@Override
	public int hashCode() {
		return Objects.hash(timestamp, value);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Sample other = (Sample) obj;
		return timestamp == other.timestamp && Double.doubleToLongBits(value) == Double.doubleToLongBits(other.value);
	}

}