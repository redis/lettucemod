package com.redislabs.mesclun.timeseries.protocol;

import java.nio.charset.StandardCharsets;

import io.lettuce.core.protocol.ProtocolKeyword;

/**
 * RedisTimeSeries commands.
 *
 * @author Julien Ruaux
 */
public enum CommandType implements ProtocolKeyword {

	ADD, CREATE, CREATERULE, DELETERULE;

	private final static String PREFIX = "TS.";

	public final byte[] bytes;

	CommandType() {
		bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}
}
