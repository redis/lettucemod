package com.redis.lettucemod.timeseries;

import java.time.Duration;
import java.util.Optional;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.CommandArgs;

public class CreateRuleOptions implements CompositeArgument {

	private final Aggregator aggregator;
	private final Duration bucketDuration;
	private final Optional<Duration> alignTimestamp;

	private CreateRuleOptions(Builder builder) {
		this.aggregator = builder.aggregator;
		this.bucketDuration = builder.bucketDuration;
		this.alignTimestamp = builder.alignTimestamp;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		aggregator.build(args);
		args.add(bucketDuration.toMillis());
		alignTimestamp.ifPresent(a -> args.add(a.toMillis()));
	}

	public static BucketDurationBuilder builder(Aggregator aggregator) {
		return new BucketDurationBuilder(aggregator);
	}

	public static class BucketDurationBuilder {
		private final Aggregator aggregator;

		public BucketDurationBuilder(Aggregator aggregator) {
			this.aggregator = aggregator;
		}

		public Builder bucketDuration(Duration duration) {
			LettuceAssert.notNull(duration, "Bucket duration must not be null");
			LettuceAssert.isTrue(!duration.isNegative() && !duration.isZero(), "Bucket duration must be positive");
			return new Builder(aggregator, duration);
		}
	}

	public static class Builder {
		private final Aggregator aggregator;
		private final Duration bucketDuration;
		private Optional<Duration> alignTimestamp = Optional.empty();

		private Builder(Aggregator aggregator, Duration bucketDuration) {
			this.aggregator = aggregator;
			this.bucketDuration = bucketDuration;
		}

		public Builder alignTimestamp(Duration timestamp) {
			LettuceAssert.notNull(timestamp, "Align timestamp must not be null");
			LettuceAssert.isTrue(!timestamp.isNegative(), "Align timestamp must be positive");
			this.alignTimestamp = Optional.of(timestamp);
			return this;
		}

		public CreateRuleOptions build() {
			return new CreateRuleOptions(this);
		}
	}

}
