package com.redis.lettucemod.bloom;

import java.util.OptionalLong;

import com.redis.lettucemod.protocol.BloomCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class TDigestMergeOptions implements CompositeArgument {

	private OptionalLong compression = OptionalLong.empty();
	private boolean override;

	public TDigestMergeOptions() {
	}

	private TDigestMergeOptions(Builder builder) {
		this.compression = builder.compression;
		this.override = builder.override;
	}

	public OptionalLong getCompression() {
		return compression;
	}

	public void setCompression(OptionalLong compression) {
		this.compression = compression;
	}

	public boolean isOverride() {
		return override;
	}

	public void setOverride(boolean override) {
		this.override = override;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> commandArgs) {
		compression.ifPresent(c -> commandArgs.add(BloomCommandKeyword.COMPRESSION).add(c));
		if (override) {
			commandArgs.add(BloomCommandKeyword.OVERRIDE);
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private OptionalLong compression = OptionalLong.empty();
		private boolean override;

		public Builder compression(long compression) {
			this.compression = OptionalLong.of(compression);
			return this;
		}

		public Builder override(boolean override) {
			this.override = override;
			return this;
		}

		public TDigestMergeOptions build() {
			return new TDigestMergeOptions(this);
		}
	}
}
