package com.redis.lettucemod.timeseries;

import java.util.Optional;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.protocol.CommandArgs;

abstract class AbstractAddCreateOptions<K, V> extends AbstractAddAlterCreateIncrbyOptions<K, V> {

	private final Optional<Encoding> encoding;
	private final Optional<DuplicatePolicy> duplicatePolicy;

	protected AbstractAddCreateOptions(Builder<K, V, ?> builder) {
		super(builder);
		this.encoding = builder.encoding;
		this.duplicatePolicy = builder.policy;
	}

	protected <L, W> void buildDuplicatePolicy(CommandArgs<L, W> args, TimeSeriesCommandKeyword keyword) {
		duplicatePolicy.ifPresent(p -> args.add(keyword).add(p.getKeyword()));
	}

	protected <L, W> void buildEncoding(CommandArgs<L, W> args) {
		encoding.ifPresent(e -> args.add(TimeSeriesCommandKeyword.ENCODING).add(e.getKeyword()));
	}

	@SuppressWarnings("unchecked")
	public static class Builder<K, V, B extends Builder<K, V, B>>
			extends AbstractAddAlterCreateIncrbyOptions.Builder<K, V, B> {

		private Optional<Encoding> encoding = Optional.empty();
		private Optional<DuplicatePolicy> policy = Optional.empty();

		public B encoding(Encoding encoding) {
			this.encoding = Optional.of(encoding);
			return (B) this;
		}

		public B policy(DuplicatePolicy policy) {
			this.policy = Optional.of(policy);
			return (B) this;
		}

	}

}
