package com.redis.lettucemod.bloom;

import java.util.OptionalInt;

import com.redis.lettucemod.protocol.BloomCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class BloomFilterReserveOptions implements CompositeArgument {

	private OptionalInt expansion = OptionalInt.empty();
	private boolean nonScaling;

	public BloomFilterReserveOptions() {
	}

	private BloomFilterReserveOptions(Builder builder) {
		this.nonScaling = builder.nonScaling;
		this.expansion = builder.expansion;
	}

	public OptionalInt getExpansion() {
		return expansion;
	}

	public void setExpansion(OptionalInt expansion) {
		this.expansion = expansion;
	}

	public boolean isNonScaling() {
		return nonScaling;
	}

	public void setNonScaling(boolean nonScaling) {
		this.nonScaling = nonScaling;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		expansion.ifPresent(e -> args.add(BloomCommandKeyword.EXPANSION).add(e));
		if (nonScaling) {
			args.add(BloomCommandKeyword.NONSCALING);
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private OptionalInt expansion = OptionalInt.empty();
		private boolean nonScaling;

		public Builder nonScaling(boolean nonScaling) {
			this.nonScaling = nonScaling;
			return this;
		}

		public Builder expansion(int expansion) {
			this.expansion = OptionalInt.of(expansion);
			return this;
		}

		public BloomFilterReserveOptions build() {
			return new BloomFilterReserveOptions(this);
		}

	}

}
