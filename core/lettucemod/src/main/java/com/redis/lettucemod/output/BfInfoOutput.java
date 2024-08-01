package com.redis.lettucemod.output;

import com.redis.lettucemod.bloom.BloomFilterInfo;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

public class BfInfoOutput<K, V> extends CommandOutput<K, V, BloomFilterInfo> {

	private String field;

	public BfInfoOutput(RedisCodec<K, V> codec) {
		super(codec, new BloomFilterInfo());
	}

	@Override
	public void set(ByteBuffer buffer) {
		field = decodeAscii(buffer);
	}

	private boolean fieldEquals(String name) {
		return name.equals(field);
	}

	@Override
	public void set(long integer) {
		if (fieldEquals("Capacity")) {
			output.setCapacity(integer);
		} else if (fieldEquals("Size")) {
			output.setSize(integer);
		} else if (fieldEquals("Number of filters")) {
			output.setNumFilters(integer);
		} else if (fieldEquals("Number of items inserted")) {
			output.setNumInserted(integer);
		} else if (fieldEquals("Expansion rate")) {
			output.setExpansionRate(integer);
		}

	}
}
