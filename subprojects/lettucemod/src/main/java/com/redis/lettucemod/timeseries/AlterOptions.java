package com.redis.lettucemod.timeseries;

import java.util.Optional;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.protocol.CommandArgs;

public class AlterOptions<K, V> extends BaseOptions<K, V> {

	private final Optional<DuplicatePolicy> duplicatePolicy;

	protected AlterOptions(Builder<K, V> builder) {
		super(builder);
		this.duplicatePolicy = builder.duplicatePolicy;
	}

	@SuppressWarnings("hiding")
	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		super.build(args);
		duplicatePolicy.ifPresent(p -> args.add(TimeSeriesCommandKeyword.DUPLICATE_POLICY).add(p.getKeyword()));
	}

	public static class Builder<K, V> extends BaseOptions.Builder<K, V, Builder<K, V>> {

		private Optional<DuplicatePolicy> duplicatePolicy = Optional.empty();

		public Builder<K, V> policy(DuplicatePolicy policy) {
			this.duplicatePolicy = Optional.of(policy);
			return this;
		}

	}

}
