package com.redis.lettucemod.timeseries;

import java.util.Optional;
import java.util.OptionalLong;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class CreateOptions implements CompositeArgument {

	public enum DuplicatePolicy {
		BLOCK, FIRST, LAST, MIN, MAX, SUM
	}

	private OptionalLong retentionTime = OptionalLong.empty();
	private boolean uncompressed;
	private OptionalLong chunkSize = OptionalLong.empty();
	private Optional<DuplicatePolicy> policy = Optional.empty();

	private CreateOptions(Builder builder) {
		this.retentionTime = builder.retentionTime;
		this.uncompressed = builder.uncompressed;
		this.chunkSize = builder.chunkSize;
		this.policy = builder.policy;
	}

	@Override
	public <L, W> void build(CommandArgs<L, W> args) {
		retentionTime.ifPresent(t -> args.add(TimeSeriesCommandKeyword.RETENTION).add(t));
		if (uncompressed) {
			args.add(TimeSeriesCommandKeyword.UNCOMPRESSED);
		}
		chunkSize.ifPresent(s -> args.add(TimeSeriesCommandKeyword.CHUNK_SIZE).add(s));
		policy.ifPresent(p -> args.add(TimeSeriesCommandKeyword.ON_DUPLICATE).add(p.name()));
	}

	public static <K, V> Builder builder() {
		return new Builder();
	}

	public static final class Builder {
		private OptionalLong retentionTime = OptionalLong.empty();
		private boolean uncompressed;
		private OptionalLong chunkSize = OptionalLong.empty();
		private Optional<DuplicatePolicy> policy = Optional.empty();

		private Builder() {
		}

		public Builder retentionTime(long retentionTime) {
			this.retentionTime = OptionalLong.of(retentionTime);
			return this;
		}

		public Builder uncompressed(boolean uncompressed) {
			this.uncompressed = uncompressed;
			return this;
		}

		public Builder chunkSize(long chunkSize) {
			this.chunkSize = OptionalLong.of(chunkSize);
			return this;
		}

		public Builder policy(DuplicatePolicy policy) {
			this.policy = Optional.of(policy);
			return this;
		}

		public CreateOptions build() {
			return new CreateOptions(this);
		}
	}

}
