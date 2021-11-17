package com.redis.lettucemod.search;

public class Suggestion<V> {

	private V string;
	private Double score;
	private V payload;

	public V getString() {
		return string;
	}

	public void setString(V string) {
		this.string = string;
	}

	public Double getScore() {
		return score;
	}

	public void setScore(Double score) {
		this.score = score;
	}

	public V getPayload() {
		return payload;
	}

	public void setPayload(V payload) {
		this.payload = payload;
	}

}
