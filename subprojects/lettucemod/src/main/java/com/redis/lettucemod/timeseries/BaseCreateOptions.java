package com.redis.lettucemod.timeseries;

import java.util.Optional;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.protocol.CommandArgs;

public class BaseCreateOptions<K, V> extends BaseOptions<K, V> {

	private final Optional<Encoding> encoding;
	private final Optional<DuplicatePolicy> duplicatePolicy;
	private final TimeSeriesCommandKeyword keyword;

	protected BaseCreateOptions(TimeSeriesCommandKeyword keyword, Builder<K, V, ?> builder) {
		super(builder);
		this.keyword = keyword;
		this.encoding = builder.encoding;
		this.duplicatePolicy = builder.policy;
	}

	@SuppressWarnings("hiding")
	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		super.build(args);
		duplicatePolicy.ifPresent(p -> args.add(keyword).add(p.getKeyword()));
		encoding.ifPresent(e -> args.add(TimeSeriesCommandKeyword.ENCODING).add(e.getKeyword()));
	}

	@SuppressWarnings("unchecked")
	public static class Builder<K, V, B extends Builder<K, V, B>> extends BaseOptions.Builder<K, V, B> {

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
