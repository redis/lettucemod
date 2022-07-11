package com.redis.lettucemod.search;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import io.lettuce.core.internal.LettuceAssert;

public class Document<K, V> extends LinkedHashMap<K, V> {

	private static final long serialVersionUID = 1L;

	private K id;
	private Double score;
	private V sortKey;
	private V payload;

	public K getId() {
		return id;
	}

	public void setId(K id) {
		this.id = id;
	}

	public Double getScore() {
		return score;
	}

	public void setScore(Double score) {
		this.score = score;
	}

	public V getSortKey() {
		return sortKey;
	}

	public void setSortKey(V sortKey) {
		this.sortKey = sortKey;
	}

	public V getPayload() {
		return payload;
	}

	public void setPayload(V payload) {
		this.payload = payload;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(id, payload, score, sortKey);
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Document<K, V> other = (Document<K, V>) obj;
		return Objects.equals(id, other.id) && Objects.equals(payload, other.payload)
				&& Objects.equals(score, other.score) && Objects.equals(sortKey, other.sortKey);
	}

	public static <K, V> Builder<K, V> id(K id) {
		return new Builder<>(id);
	}

	public static class Builder<K, V> {

		private final K id;
		private double score = 1;
		private V payload;
		private Map<K, V> fields = new HashMap<>();

		public Builder(K id) {
			super();
			this.id = id;
		}

		public Builder<K, V> score(double score) {
			this.score = score;
			return this;
		}

		public Builder<K, V> payload(V payload) {
			this.payload = payload;
			return this;
		}

		public Builder<K, V> field(K name, V value) {
			fields.put(name, value);
			return this;
		}

		public Document<K, V> build() {
			LettuceAssert.notNull(id, "Id is required.");
			LettuceAssert.notNull(fields, "Fields are required.");
			Document<K, V> document = new Document<>();
			document.setId(id);
			document.setScore(score);
			document.setPayload(payload);
			document.putAll(fields);
			return document;
		}

	}

}
