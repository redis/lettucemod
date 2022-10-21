package com.redis.lettucemod.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

/**
 * RedisJSON commands.
 *
 * @author Julien Ruaux
 */
public enum JsonCommandType implements ProtocolKeyword {

    DEL, GET, MGET, SET, TYPE, NUMINCRBY, NUMMULTBY, STRAPPEND, STRLEN, ARRAPPEND, ARRINDEX, ARRINSERT, ARRLEN, ARRPOP, ARRTRIM, OBJKEYS, OBJLEN;

    private static final String PREFIX = "JSON.";

    private final byte[] bytes;

    JsonCommandType() {
        bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
