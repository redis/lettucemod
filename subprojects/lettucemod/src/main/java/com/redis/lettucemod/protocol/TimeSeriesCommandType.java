package com.redis.lettucemod.protocol;

import java.nio.charset.StandardCharsets;

import io.lettuce.core.protocol.ProtocolKeyword;

/**
 * RedisTimeSeries commands.
 *
 * @author Julien Ruaux
 */
public enum TimeSeriesCommandType implements ProtocolKeyword {

	ADD, ALTER, CREATE, CREATERULE, DELETERULE, MADD, INCRBY, DECRBY, RANGE, REVRANGE, MRANGE, MREVRANGE, GET, MGET, INFO, QUERYINDEX, DEL;

	private static final String PREFIX = "TS.";

	private final byte[] bytes;

	TimeSeriesCommandType() {
		bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}
}
