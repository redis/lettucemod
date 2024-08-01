package com.redis.lettucemod.protocol;

import java.nio.charset.StandardCharsets;

import io.lettuce.core.protocol.ProtocolKeyword;

public enum BloomCommandKeyword implements ProtocolKeyword {

	CAPACITY, ERROR, NONSCALING, EXPANSION, NOCREATE, ITEMS, BUCKETSIZE, MAXITERATIONS, WEIGHTS, WITHCOUNT, COMPRESSION,
	OVERRIDE;

	final byte[] bytes;

	BloomCommandKeyword() {
		bytes = name().getBytes(StandardCharsets.US_ASCII);
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}
}
