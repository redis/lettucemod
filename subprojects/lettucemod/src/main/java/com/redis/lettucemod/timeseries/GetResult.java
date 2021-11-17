package com.redis.lettucemod.timeseries;

import java.util.Map;

public class GetResult<K, V> {

	private K key;
	private Map<K, V> labels;
	private Sample sample;

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public Map<K, V> getLabels() {
		return labels;
	}

	public void setLabels(Map<K, V> labels) {
		this.labels = labels;
	}

	public Sample getSample() {
		return sample;
	}

	public void setSample(Sample sample) {
		this.sample = sample;
	}

}
