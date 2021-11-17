package com.redis.lettucemod.timeseries;

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

}
