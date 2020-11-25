package com.redislabs.mesclun.timeseries.protocol;

import java.nio.charset.StandardCharsets;

import io.lettuce.core.protocol.ProtocolKeyword;

public enum CommandKeyword implements ProtocolKeyword {

	RETENTION, UNCOMPRESSED, CHUNK_SIZE, ON_DUPLICATE, LABELS;

	public final byte[] bytes;

	CommandKeyword() {
		bytes = name().getBytes(StandardCharsets.US_ASCII);
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}
}
