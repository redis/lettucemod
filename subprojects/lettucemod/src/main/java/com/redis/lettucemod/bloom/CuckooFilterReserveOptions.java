package com.redis.lettucemod.bloom;

import java.util.OptionalLong;

import com.redis.lettucemod.protocol.BloomCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class CuckooFilterReserveOptions implements CompositeArgument {

	private OptionalLong bucketSize = OptionalLong.empty();
	private OptionalLong maxIterations = OptionalLong.empty();
	private OptionalLong expansion = OptionalLong.empty();

	public CuckooFilterReserveOptions() {
	}

	private CuckooFilterReserveOptions(Builder builder) {
		this.bucketSize = builder.bucketSize;
		this.maxIterations = builder.maxIterations;
		this.expansion = builder.expansion;
	}

	public OptionalLong getBucketSize() {
		return bucketSize;
	}

	public void setBucketSize(OptionalLong bucketSize) {
		this.bucketSize = bucketSize;
	}

	public OptionalLong getMaxIterations() {
		return maxIterations;
	}

	public void setMaxIterations(OptionalLong maxIterations) {
		this.maxIterations = maxIterations;
	}

	public OptionalLong getExpansion() {
		return expansion;
	}

	public void setExpansion(OptionalLong expansion) {
		this.expansion = expansion;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> commandArgs) {
		bucketSize.ifPresent(s -> commandArgs.add(BloomCommandKeyword.BUCKETSIZE).add(s));
		maxIterations.ifPresent(i -> commandArgs.add(BloomCommandKeyword.MAXITERATIONS).add(i));
		expansion.ifPresent(e -> commandArgs.add(BloomCommandKeyword.EXPANSION).add(e));
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private OptionalLong bucketSize = OptionalLong.empty();
		private OptionalLong maxIterations = OptionalLong.empty();
		private OptionalLong expansion = OptionalLong.empty();

		public Builder bucketSize(long size) {
			this.bucketSize = OptionalLong.of(size);
			return this;
		}

		public Builder maxIterations(long iterations) {
			this.maxIterations = OptionalLong.of(iterations);
			return this;
		}

		public Builder expansion(long expansion) {
			this.expansion = OptionalLong.of(expansion);
			return this;
		}

		public CuckooFilterReserveOptions build() {
			return new CuckooFilterReserveOptions(this);
		}
	}
}
