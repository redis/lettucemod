package com.redis.lettucemod.timeseries;

import java.util.List;
import java.util.Map;

public class RangeResult<K, V> {

	private K key;
	private Map<K, V> labels;
	private List<Sample> samples;

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

	public List<Sample> getSamples() {
		return samples;
	}

	public void setSamples(List<Sample> samples) {
		this.samples = samples;
	}

}
