package com.redis.lettucemod.bloom;

import java.util.OptionalLong;

import com.redis.lettucemod.protocol.BloomCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class CuckooFilterInsertOptions implements CompositeArgument {

	private OptionalLong capacity = OptionalLong.empty();
	private boolean noCreate;

	public CuckooFilterInsertOptions() {
	}

	private CuckooFilterInsertOptions(Builder builder) {
		this.capacity = builder.capacity;
		this.noCreate = builder.noCreate;
	}

	public OptionalLong getCapacity() {
		return capacity;
	}

	public void setCapacity(OptionalLong capacity) {
		this.capacity = capacity;
	}

	public boolean isNoCreate() {
		return noCreate;
	}

	public void setNoCreate(boolean noCreate) {
		this.noCreate = noCreate;
	}

	@Override
	public <K, V> void build(CommandArgs<K, V> commandArgs) {
		capacity.ifPresent(c -> commandArgs.add(BloomCommandKeyword.CAPACITY).add(c));
		if (noCreate) {
			commandArgs.add(BloomCommandKeyword.NOCREATE);
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {

		private OptionalLong capacity = OptionalLong.empty();
		private boolean noCreate;

		public Builder capacity(long capacity) {
			this.capacity = OptionalLong.of(capacity);
			return this;
		}

		public Builder noCreate(boolean noCreate) {
			this.noCreate = noCreate;
			return this;
		}

		public CuckooFilterInsertOptions build() {
			return new CuckooFilterInsertOptions(this);
		}
	}
}
