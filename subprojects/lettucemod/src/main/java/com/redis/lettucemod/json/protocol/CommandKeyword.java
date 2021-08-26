package com.redis.lettucemod.json.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

public enum CommandKeyword implements ProtocolKeyword {

	INDENT, NEWLINE, SPACE, NOESCAPE, NX, XX;

	public final byte[] bytes;

	CommandKeyword() {
		bytes = name().getBytes(StandardCharsets.US_ASCII);
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}
}
