package com.redis.lettucemod.protocol;

import java.nio.charset.StandardCharsets;

import io.lettuce.core.protocol.ProtocolKeyword;

public enum TimeSeriesCommandKeyword implements ProtocolKeyword {

	RETENTION, UNCOMPRESSED, CHUNK_SIZE, ON_DUPLICATE, LABELS, AGGREGATION, TIMESTAMP, COUNT, WITHLABELS, FILTER, DEBUG;

	public final byte[] bytes;

	TimeSeriesCommandKeyword() {
		bytes = name().getBytes(StandardCharsets.US_ASCII);
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}
}
