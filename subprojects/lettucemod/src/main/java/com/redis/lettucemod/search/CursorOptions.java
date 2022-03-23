package com.redis.lettucemod.search;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import com.redis.lettucemod.protocol.SearchCommandArgs;
import com.redis.lettucemod.protocol.SearchCommandKeyword;

public class CursorOptions {

	private OptionalLong count = OptionalLong.empty();
	private Optional<Duration> maxIdle = Optional.empty();

	public CursorOptions() {
	}

	private CursorOptions(Builder builder) {
		this.count = builder.count;
		this.maxIdle = builder.maxIdle;
	}

	public <K, V> void build(SearchCommandArgs<K, V> args) {
		count.ifPresent(c -> {
			args.add(SearchCommandKeyword.COUNT);
			args.add(c);
		});
		maxIdle.ifPresent(m -> {
			args.add(SearchCommandKeyword.MAXIDLE);
			args.add(m.toMillis());
		});
	}

	public OptionalLong getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = OptionalLong.of(count);
	}

	public Optional<Duration> getMaxIdle() {
		return maxIdle;
	}

	public void setMaxIdle(Duration maxIdle) {
		this.maxIdle = Optional.of(maxIdle);
	}

	@Override
	public int hashCode() {
		return Objects.hash(count, maxIdle);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CursorOptions other = (CursorOptions) obj;
		return Objects.equals(count, other.count) && Objects.equals(maxIdle, other.maxIdle);
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private OptionalLong count = OptionalLong.empty();
		private Optional<Duration> maxIdle = Optional.empty();

		private Builder() {
		}

		public Builder count(long count) {
			this.count = OptionalLong.of(count);
			return this;
		}

		public Builder maxIdle(Duration maxIdle) {
			this.maxIdle = Optional.of(maxIdle);
			return this;
		}

		public CursorOptions build() {
			return new CursorOptions(this);
		}
	}

}
