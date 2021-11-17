package com.redis.lettucemod.search;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

@SuppressWarnings("rawtypes")
public class SuggetOptions implements RediSearchArgument {

	private boolean fuzzy;
	private boolean withScores;
	private boolean withPayloads;
	private Long max;

	private SuggetOptions(Builder builder) {
		this.fuzzy = builder.fuzzy;
		this.withScores = builder.withScores;
		this.withPayloads = builder.withPayloads;
		this.max = builder.max;
	}

	public boolean isFuzzy() {
		return fuzzy;
	}

	public void setFuzzy(boolean fuzzy) {
		this.fuzzy = fuzzy;
	}

	public boolean isWithScores() {
		return withScores;
	}

	public void setWithScores(boolean withScores) {
		this.withScores = withScores;
	}

	public boolean isWithPayloads() {
		return withPayloads;
	}

	public void setWithPayloads(boolean withPayloads) {
		this.withPayloads = withPayloads;
	}

	public Long getMax() {
		return max;
	}

	public void setMax(Long max) {
		this.max = max;
	}

	@Override
	public void build(SearchCommandArgs args) {
		if (fuzzy) {
			args.add(SearchCommandKeyword.FUZZY);
		}
		if (withScores) {
			args.add(SearchCommandKeyword.WITHSCORES);
		}
		if (withPayloads) {
			args.add(SearchCommandKeyword.WITHPAYLOADS);
		}
		if (max != null) {
			args.add(SearchCommandKeyword.MAX);
			args.add(max);
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {
		private boolean fuzzy;
		private boolean withScores;
		private boolean withPayloads;
		private Long max;

		private Builder() {
		}

		public Builder fuzzy(boolean fuzzy) {
			this.fuzzy = fuzzy;
			return this;
		}

		public Builder withScores(boolean withScores) {
			this.withScores = withScores;
			return this;
		}

		public Builder withPayloads(boolean withPayloads) {
			this.withPayloads = withPayloads;
			return this;
		}

		public Builder max(Long max) {
			this.max = max;
			return this;
		}

		public SuggetOptions build() {
			return new SuggetOptions(this);
		}
	}

}
