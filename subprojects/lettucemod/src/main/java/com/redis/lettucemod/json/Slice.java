package com.redis.lettucemod.json;

import java.util.OptionalLong;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class Slice implements CompositeArgument {

	private final long start;
	private OptionalLong stop;

	private Slice(Builder builder) {
		this.start = builder.start;
		this.stop = builder.stop;
	}

	public long getStart() {
		return start;
	}

	public OptionalLong getStop() {
		return stop;
	}

	public static Builder start(long start) {
		return new Builder(start);
	}

	public static final class Builder {
		private final long start;
		private OptionalLong stop = OptionalLong.empty();

		private Builder(long start) {
			this.start = start;
		}

		public Slice stop(long stop) {
			this.stop = OptionalLong.of(stop);
			return new Slice(this);
		}

	}

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		args.add(start);
		stop.ifPresent(args::add);
	}

}
