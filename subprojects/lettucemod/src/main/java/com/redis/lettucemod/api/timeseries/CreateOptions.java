package com.redis.lettucemod.api.timeseries;

import com.redis.lettucemod.protocol.TimeSeriesCommandKeyword;
import io.lettuce.core.CompositeArgument;
import io.lettuce.core.protocol.CommandArgs;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

import java.util.Map;

@Data
@Builder
public class CreateOptions<K, V> implements CompositeArgument {

    private Long retentionTime;
    private boolean uncompressed;
    private Long chunkSize;
    private DuplicatePolicy policy;
    @Singular
    private Map<K, V> labels;

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void build(CommandArgs<K, V> args) {
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
            labels.forEach((k, v) -> args.addKey((K) k).addValue((V) v));
        }
    }

    public static class CreateOptionsBuilder<K, V> {

        public CreateOptionsBuilder<K, V> retentionTime(long retentionTime) {
            this.retentionTime = retentionTime;
            return this;
        }

        public CreateOptionsBuilder<K, V> chunkSize(long chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }
    }

    public enum DuplicatePolicy {

        BLOCK, FIRST, LAST, MIN, MAX, SUM

    }
}
