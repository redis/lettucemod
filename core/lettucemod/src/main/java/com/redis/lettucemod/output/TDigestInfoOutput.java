package com.redis.lettucemod.output;

import com.redis.lettucemod.bloom.TDigestInfo;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

public class TDigestInfoOutput<K, V> extends CommandOutput<K, V, TDigestInfo> {

	private String field;

	public TDigestInfoOutput(RedisCodec<K, V> codec) {
		super(codec, new TDigestInfo());
	}

	@Override
	public void set(ByteBuffer buffer) {
		field = decodeString(buffer);
	}

	@Override
	public void set(long integer) {
		switch (field) {
		case "Compression":
			output.setCompression(integer);
			break;
		case "Capacity":
			output.setCapacity(integer);
			break;
		case "Merged nodes":
			output.setMergedNodes(integer);
			break;
		case "Unmerged nodes":
			output.setUnmergedNodes(integer);
			break;
		case "Merged weight":
			output.setMergedWeight(integer);
			break;
		case "Unmerged weight":
			output.setUnmergedWeight(integer);
			break;
		case "Observations":
			output.setObservations(integer);
			break;
		case "Total compressions":
			output.setTotalCompressions(integer);
			break;
		case "Memory usage":
			output.setMemoryUsage(integer);
			break;
		default:
			break;
		}
	}
}
