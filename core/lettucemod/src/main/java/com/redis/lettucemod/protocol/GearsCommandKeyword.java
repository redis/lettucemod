package com.redis.lettucemod.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

public enum GearsCommandKeyword implements ProtocolKeyword {

	UNBLOCKING, REQUIREMENTS, SHARD, CLUSTER;

	private final byte[] bytes;

	GearsCommandKeyword() {
		bytes = name().getBytes(StandardCharsets.US_ASCII);
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}
}
