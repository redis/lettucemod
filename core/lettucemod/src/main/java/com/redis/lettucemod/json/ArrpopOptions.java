package com.redis.lettucemod.json;

import java.util.OptionalLong;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class ArrpopOptions<K> implements CompositeArgument {

	private final K path;
	private OptionalLong index;

	private ArrpopOptions(Builder<K> builder) {
		this.path = builder.path;
		this.index = builder.index;
	}

	public K getPath() {
		return path;
	}

	public OptionalLong getIndex() {
		return index;
	}

	public void setIndex(OptionalLong index) {
		this.index = index;
	}

	public static <K> Builder<K> path(K path) {
		return new Builder<>(path);
	}

	public static final class Builder<K> {
		private final K path;
		private OptionalLong index = OptionalLong.empty();

		private Builder(K path) {
			this.path = path;
		}

		public ArrpopOptions<K> index(long index) {
			this.index = OptionalLong.of(index);
			return new ArrpopOptions<>(this);
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public <L, W> void build(CommandArgs<L, W> args) {
		args.addKey((L) path);
		index.ifPresent(args::add);
	}

}
