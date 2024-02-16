package com.redis.lettucemod.timeseries;

import java.util.List;

import io.lettuce.core.KeyValue;

public class GetResult<K, V> {

	private K key;
	private List<KeyValue<K, V>> labels;
	private Sample sample;

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public List<KeyValue<K, V>> getLabels() {
		return labels;
	}

	public void setLabels(List<KeyValue<K, V>> labels) {
		this.labels = labels;
	}

	public Sample getSample() {
		return sample;
	}

	public void setSample(Sample sample) {
		this.sample = sample;
	}

}
