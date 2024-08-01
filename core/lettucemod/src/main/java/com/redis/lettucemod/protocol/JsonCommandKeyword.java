package com.redis.lettucemod.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

public enum JsonCommandKeyword implements ProtocolKeyword {

	INDENT, NEWLINE, SPACE, NOESCAPE, NX, XX;

	private final byte[] bytes;

	JsonCommandKeyword() {
		bytes = name().getBytes(StandardCharsets.US_ASCII);
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}
}
