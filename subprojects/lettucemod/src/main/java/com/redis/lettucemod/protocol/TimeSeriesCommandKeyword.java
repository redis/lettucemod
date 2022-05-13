package com.redis.lettucemod.protocol;

import java.nio.charset.StandardCharsets;

import io.lettuce.core.protocol.ProtocolKeyword;

public enum TimeSeriesCommandKeyword implements ProtocolKeyword {

	RETENTION, UNCOMPRESSED, CHUNK_SIZE, ON_DUPLICATE, LABELS, AGGREGATION, TIMESTAMP, COUNT, WITHLABELS,
	SELECTED_LABELS, FILTER, DEBUG, FILTER_BY_TS, FILTER_BY_VALUE, ALIGN, BUCKETTIMESTAMP, EMPTY, GROUPBY, REDUCE,
	START("-"), END("+");

	public final byte[] bytes;

	TimeSeriesCommandKeyword(String keyword) {
		bytes = keyword.getBytes(StandardCharsets.US_ASCII);
	}

	TimeSeriesCommandKeyword() {
		bytes = name().getBytes(StandardCharsets.US_ASCII);
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}
}
