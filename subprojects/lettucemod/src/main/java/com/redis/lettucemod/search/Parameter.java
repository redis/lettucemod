package com.redis.lettucemod.search;

public class Parameter<K, V> {

	private K name;
	private V value;

	public K getName() {
		return name;
	}

	public void setName(K name) {
		this.name = name;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

	public static <K, V> Parameter<K, V> of(K name, V value) {
		Parameter<K, V> l = new Parameter<>();
		l.setName(name);
		l.setValue(value);
		return l;
	}

}
