package com.redis.lettucemod.bloom;

import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;

import com.redis.lettucemod.protocol.BloomCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class BloomFilterInsertOptions implements CompositeArgument {

	private OptionalLong capacity = OptionalLong.empty();
	private OptionalDouble error = OptionalDouble.empty();
	private boolean nonScaling;
	private boolean noCreate;
	private OptionalInt expansion = OptionalInt.empty();

	public BloomFilterInsertOptions() {
	}

	private BloomFilterInsertOptions(Builder builder) {
		this.capacity = builder.capacity;
		this.error = builder.error;
		this.noCreate = builder.noCreate;
		this.nonScaling = builder.nonScaling;
		this.expansion = builder.expansion;
	}

	public OptionalLong getCapacity() {
		return capacity;
	}

	public void setCapacity(OptionalLong capacity) {
		this.capacity = capacity;
	}

	public OptionalDouble getError() {
		return error;
	}

	public void setError(OptionalDouble error) {
		this.error = error;
	}

	public boolean isNonScaling() {
		return nonScaling;
	}

	public void setNonScaling(boolean nonScaling) {
		this.nonScaling = nonScaling;
	}

	public boolean isNoCreate() {
		return noCreate;
	}

	public void setNoCreate(boolean noCreate) {
		this.noCreate = noCreate;
	}

	public OptionalInt getExpansion() {
		return expansion;
	}

	public void setExpansion(OptionalInt expansion) {
		this.expansion = expansion;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		capacity.ifPresent(c -> args.add(BloomCommandKeyword.CAPACITY).add(c));
		error.ifPresent(e -> args.add(BloomCommandKeyword.ERROR).add(e));
		expansion.ifPresent(e -> args.add(BloomCommandKeyword.EXPANSION).add(e));
		if (noCreate) {
			args.add(BloomCommandKeyword.NOCREATE);
		}
		if (nonScaling) {
			args.add(BloomCommandKeyword.NONSCALING);
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private OptionalLong capacity = OptionalLong.empty();
		private OptionalDouble error = OptionalDouble.empty();
		private boolean nonScaling;
		private boolean noCreate;
		private OptionalInt expansion = OptionalInt.empty();

		public Builder capacity(long capacity) {
			this.capacity = OptionalLong.of(capacity);
			return this;
		}

		public Builder error(double error) {
			this.error = OptionalDouble.of(error);
			return this;
		}

		public Builder nonScaling(boolean nonScaling) {
			this.nonScaling = nonScaling;
			return this;
		}

		public Builder noCreate(boolean noCreate) {
			this.noCreate = noCreate;
			return this;
		}

		public Builder expansion(int expansion) {
			this.expansion = OptionalInt.of(expansion);
			return this;
		}

		public BloomFilterInsertOptions build() {
			return new BloomFilterInsertOptions(this);
		}

	}

}
