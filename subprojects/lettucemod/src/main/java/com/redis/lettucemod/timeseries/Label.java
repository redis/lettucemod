package com.redis.lettucemod.timeseries;

public class Label<K, V> {

	private K label;
	private V value;

	public K getLabel() {
		return label;
	}

	public void setLabel(K label) {
		this.label = label;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

	public static <K, V> Label<K, V> of(K label, V value) {
		Label<K, V> l = new Label<>();
		l.setLabel(label);
		l.setValue(value);
		return l;
	}

}
