package com.redis.lettucemod.protocol;

import java.nio.charset.StandardCharsets;

import io.lettuce.core.protocol.ProtocolKeyword;

/**
 * RediSearch commands.
 *
 * @author Julien Ruaux
 */
public enum SearchCommandType implements ProtocolKeyword {

	AGGREGATE, ALTER, CREATE, CURSOR, DROPINDEX, INFO, SEARCH, SUGADD, SUGGET, SUGDEL, SUGLEN, ALIASADD, ALIASUPDATE,
	ALIASDEL, LIST("_LIST"), TAGVALS, DICTADD, DICTDEL, DICTDUMP;

	private static final String PREFIX = "FT.";

	private final byte[] bytes;

	SearchCommandType() {
		bytes = bytes(name());
	}

	SearchCommandType(String name) {
		bytes = bytes(name);
	}

	static byte[] bytes(String name) {
		return (PREFIX + name).getBytes(StandardCharsets.US_ASCII);
	}

	@Override
	public byte[] getBytes() {
		return bytes;
	}
}
