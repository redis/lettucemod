package com.redis.lettucemod.timeseries;

import java.util.Optional;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.protocol.CommandArgs;

public class BaseCreateOptions<K, V> extends BaseOptions<K, V> {

	private final TimeSeriesCommandKeyword duplicatePolicyKeyword;
	private Optional<Encoding> encoding = Optional.empty();
	private Optional<DuplicatePolicy> duplicatePolicy = Optional.empty();

	public BaseCreateOptions(TimeSeriesCommandKeyword duplicatePolicyKeyword) {
		this.duplicatePolicyKeyword = duplicatePolicyKeyword;
	}

	protected BaseCreateOptions(TimeSeriesCommandKeyword duplicatePolicyKeyword, Builder<K, V, ?> builder) {
		super(builder);
		this.duplicatePolicyKeyword = duplicatePolicyKeyword;
		this.encoding = builder.encoding;
		this.duplicatePolicy = builder.policy;
	}

	public Optional<Encoding> getEncoding() {
		return encoding;
	}

	public void setEncoding(Optional<Encoding> encoding) {
		this.encoding = encoding;
	}

	@SuppressWarnings("hiding")
	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		duplicatePolicy.ifPresent(p -> args.add(duplicatePolicyKeyword).add(p.getKeyword()));
		encoding.ifPresent(e -> args.add(TimeSeriesCommandKeyword.ENCODING).add(e.getKeyword()));
		super.build(args);
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
