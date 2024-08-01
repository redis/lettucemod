package com.redis.lettucemod.timeseries;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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

	@Override
	public int hashCode() {
		return Objects.hash(key, labels, samples);
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RangeResult<K, V> other = (RangeResult<K, V>) obj;
		return Objects.equals(key, other.key) && Objects.equals(labels, other.labels)
				&& Objects.equals(samples, other.samples);
	}

}
