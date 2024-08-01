package com.redis.lettucemod.timeseries;

import java.util.Objects;

public class KeySample<K> extends Sample {

	private K key;

	public static <K> KeySample<K> of(K key, long timestamp, double value) {
		KeySample<K> sample = new KeySample<>();
		sample.setKey(key);
		sample.setTimestamp(timestamp);
		sample.setValue(value);
		return sample;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(key);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		KeySample<?> other = (KeySample<?>) obj;
		return Objects.equals(key, other.key);
	}

}
