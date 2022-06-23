package com.redis.lettucemod.timeseries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

abstract class AbstractCreateOptions<K, V> implements CompositeArgument {

	private final OptionalLong retentionPeriod;
	private final Optional<Encoding> encoding;
	private final OptionalLong chunkSize;
	private final Optional<DuplicatePolicy> duplicatePolicy;
	private final List<Label<K, V>> labels;

	protected AbstractCreateOptions(Builder<K, V, ?> builder) {
		this.retentionPeriod = builder.retentionTime;
		this.encoding = builder.encoding;
		this.chunkSize = builder.chunkSize;
		this.duplicatePolicy = builder.policy;
		this.labels = builder.labels;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void build(CommandArgs args) {
		retentionPeriod.ifPresent(t -> args.add(TimeSeriesCommandKeyword.RETENTION).add(t));
		encoding.ifPresent(e -> args.add(TimeSeriesCommandKeyword.UNCOMPRESSED));
		chunkSize.ifPresent(s -> args.add(TimeSeriesCommandKeyword.CHUNK_SIZE).add(s));
		duplicatePolicy.ifPresent(p -> args.add(getDuplicatePolicyKeyword()).add(p.getKeyword()));
		if (!labels.isEmpty()) {
			args.add(TimeSeriesCommandKeyword.LABELS);
			labels.forEach(l -> args.addKey(l.getLabel()).addValue(l.getValue()));
		}
	}

	protected abstract TimeSeriesCommandKeyword getDuplicatePolicyKeyword();

	public enum Encoding {
		COMPRESSED, UNCOMPRESSED
	}

	public static class Builder<K, V, T extends AbstractCreateOptions<K, V>> {
		private OptionalLong retentionTime = OptionalLong.empty();
		private Optional<Encoding> encoding = Optional.empty();
		private OptionalLong chunkSize = OptionalLong.empty();
		private Optional<DuplicatePolicy> policy = Optional.empty();
		private List<Label<K, V>> labels = new ArrayList<>();
		private final Function<Builder<K, V, T>, T> build;

		protected Builder(Function<Builder<K, V, T>, T> build) {
			this.build = build;
		}

		public Builder<K, V, T> retentionPeriod(long retentionPeriod) {
			this.retentionTime = OptionalLong.of(retentionPeriod);
			return this;
		}

		public Builder<K, V, T> encoding(Encoding encoding) {
			this.encoding = Optional.of(encoding);
			return this;
		}

		public Builder<K, V, T> chunkSize(long chunkSize) {
			this.chunkSize = OptionalLong.of(chunkSize);
			return this;
		}

		public Builder<K, V, T> policy(DuplicatePolicy policy) {
			this.policy = Optional.of(policy);
			return this;
		}

		@SuppressWarnings("unchecked")
		public Builder<K, V, T> labels(Label<K, V>... labels) {
			this.labels.addAll(Arrays.asList(labels));
			return this;
		}

		public T build() {
			return build.apply(this);
		}
	}

}
