package com.redis.lettucemod.timeseries;

import java.util.LinkedHashMap;
import java.util.Map;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;

import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;

public class CreateOptions<K, V> implements CompositeArgument {

	public enum DuplicatePolicy {
		BLOCK, FIRST, LAST, MIN, MAX, SUM
	}

	private Long retentionTime;
	private boolean uncompressed;
	private Long chunkSize;
	private DuplicatePolicy policy;
	private Map<K, V> labels = new LinkedHashMap<>();

	private CreateOptions(Builder<K, V> builder) {
		this.retentionTime = builder.retentionTime;
		this.uncompressed = builder.uncompressed;
		this.chunkSize = builder.chunkSize;
		this.policy = builder.policy;
		this.labels = builder.labels;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <L, W> void build(CommandArgs<L, W> args) {
		if (retentionTime != null) {
			args.add(TimeSeriesCommandKeyword.RETENTION);
			args.add(retentionTime);
		}
		if (uncompressed) {
			args.add(TimeSeriesCommandKeyword.UNCOMPRESSED);
		}
		if (chunkSize != null) {
			args.add(TimeSeriesCommandKeyword.CHUNK_SIZE);
			args.add(chunkSize);
		}
		if (policy != null) {
			args.add(TimeSeriesCommandKeyword.ON_DUPLICATE);
			args.add(policy.name());
		}
		if (labels != null) {
			args.add(TimeSeriesCommandKeyword.LABELS);
			labels.forEach((k, v) -> args.addKey((L) k).addValue((W) v));
		}
	}

	public static <K, V> Builder<K, V> builder() {
		return new Builder<>();
	}

	public static final class Builder<K, V> {
		private Long retentionTime;
		private boolean uncompressed;
		private Long chunkSize;
		private DuplicatePolicy policy;
		private Map<K, V> labels = new LinkedHashMap<>();

		private Builder() {
		}

		public Builder<K, V> retentionTime(long retentionTime) {
			this.retentionTime = retentionTime;
			return this;
		}

		public Builder<K, V> uncompressed(boolean uncompressed) {
			this.uncompressed = uncompressed;
			return this;
		}

		public Builder<K, V> chunkSize(long chunkSize) {
			this.chunkSize = chunkSize;
			return this;
		}

		public Builder<K, V> policy(DuplicatePolicy policy) {
			this.policy = policy;
			return this;
		}

		public Builder<K, V> label(K key, V value) {
			this.labels.put(key, value);
			return this;
		}

		public Builder<K, V> labels(Map<K, V> labels) {
			this.labels = labels;
			return this;
		}

		public CreateOptions<K, V> build() {
			return new CreateOptions<>(this);
		}
	}

}
