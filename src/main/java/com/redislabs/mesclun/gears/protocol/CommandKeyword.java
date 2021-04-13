package com.redislabs.mesclun.gears.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

public enum CommandKeyword implements ProtocolKeyword {

	UNBLOCKING, REQUIREMENTS, SHARD, CLUSTER;

	public final byte[] bytes;

	CommandKeyword() {
		bytes = name().getBytes(StandardCharsets.US_ASCII);
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}
}
