package com.redislabs.mesclun.timeseries;

import static com.redislabs.mesclun.timeseries.protocol.CommandKeyword.CHUNK_SIZE;
import static com.redislabs.mesclun.timeseries.protocol.CommandKeyword.ON_DUPLICATE;
import static com.redislabs.mesclun.timeseries.protocol.CommandKeyword.RETENTION;
import static com.redislabs.mesclun.timeseries.protocol.CommandKeyword.UNCOMPRESSED;

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
			args.add(RETENTION);
			args.add(retentionTime);
		}
		if (uncompressed) {
			args.add(UNCOMPRESSED);
		}
		if (chunkSize != null) {
			args.add(CHUNK_SIZE);
			args.add(chunkSize);
		}
		if (policy != null) {
			args.add(ON_DUPLICATE);
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
