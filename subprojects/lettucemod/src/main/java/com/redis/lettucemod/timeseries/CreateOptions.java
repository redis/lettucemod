package com.redis.lettucemod.timeseries;

import com.redis.lettucemod.timeseries.protocol.CommandKeyword;
import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CreateOptions implements CompositeArgument {

	private Long retentionTime;
	private boolean uncompressed;
	private Long chunkSize;
	private DuplicatePolicy policy;

	@Override
	public <K, V> void build(CommandArgs<K, V> args) {
		if (retentionTime != null) {
			args.add(CommandKeyword.RETENTION);
			args.add(retentionTime);
		}
		if (uncompressed) {
			args.add(CommandKeyword.UNCOMPRESSED);
		}
		if (chunkSize != null) {
			args.add(CommandKeyword.CHUNK_SIZE);
			args.add(chunkSize);
		}
		if (policy != null) {
			args.add(CommandKeyword.ON_DUPLICATE);
			args.add(policy.name());
		}
	}

	public static class CreateOptionsBuilder {

		public CreateOptionsBuilder retentionTime(long retentionTime) {
			this.retentionTime = retentionTime;
			return this;
		}

		public CreateOptionsBuilder chunkSize(long chunkSize) {
			this.chunkSize = chunkSize;
			return this;
		}
	}

    public enum DuplicatePolicy {

        BLOCK, FIRST, LAST, MIN, MAX, SUM

    }
}
