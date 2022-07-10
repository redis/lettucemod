package com.redis.lettucemod.search;

public class Suggestion<V> {

	private V string;
	private Double score;
	private V payload;

	public Suggestion() {
	}

	private Suggestion(Builder<V> builder) {
		this.string = builder.string;
		this.score = builder.score;
		this.payload = builder.payload;
	}

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

	public static <V> Suggestion<V> of(V string, double score) {
		Suggestion<V> suggestion = new Suggestion<>();
		suggestion.string = string;
		suggestion.score = score;
		return suggestion;
	}

	public static <V> ScoreBuilder<V> string(V string) {
		return new ScoreBuilder<>(string);
	}

	public static class ScoreBuilder<V> {
		private final V string;

		public ScoreBuilder(V string) {
			this.string = string;
		}

		public Builder<V> score(double score) {
			return new Builder<>(string, score);
		}

	}

	public static class Builder<V> {

		private final V string;
		private final double score;
		private V payload;

		public Builder(V string, double score) {
			this.string = string;
			this.score = score;
		}

		public Builder<V> payload(V payload) {
			this.payload = payload;
			return this;
		}

		public Suggestion<V> build() {
			return new Suggestion<>(this);
		}

	}

}
