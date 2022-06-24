package com.redis.lettucemod.timeseries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

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

	@SuppressWarnings("unchecked")
	public static class Builder<K, V, B extends Builder<K, V, B>> {
		private OptionalLong retentionTime = OptionalLong.empty();
		private Optional<Encoding> encoding = Optional.empty();
		private OptionalLong chunkSize = OptionalLong.empty();
		private Optional<DuplicatePolicy> policy = Optional.empty();
		private List<Label<K, V>> labels = new ArrayList<>();

		public B retentionPeriod(long retentionPeriod) {
			this.retentionTime = OptionalLong.of(retentionPeriod);
			return (B) this;
		}

		public B encoding(Encoding encoding) {
			this.encoding = Optional.of(encoding);
			return (B) this;
		}

		public B chunkSize(long chunkSize) {
			this.chunkSize = OptionalLong.of(chunkSize);
			return (B) this;
		}

		public B policy(DuplicatePolicy policy) {
			this.policy = Optional.of(policy);
			return (B) this;
		}

		public B labels(Label<K, V>... labels) {
			this.labels.addAll(Arrays.asList(labels));
			return (B) this;
		}

	}

}
