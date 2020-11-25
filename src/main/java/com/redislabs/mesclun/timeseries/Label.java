package com.redislabs.mesclun.timeseries;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

@Data
@AllArgsConstructor
public class Label<K, V> {

	@NonNull
	private K label;
	@NonNull
	private V value;

	public static <K, V> Label<K, V> of(K key, V value) {
		return new Label<>(key, value);
	}

}