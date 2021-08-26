package com.redis.lettucemod.json.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

/**
 * RedisJSON commands.
 *
 * @author Julien Ruaux
 */
public enum CommandType implements ProtocolKeyword {

    DEL, GET, MGET, SET, TYPE, NUMINCRBY, NUMMULTBY, STRAPPEND, STRLEN, ARRAPPEND, ARRINDEX, ARRINSERT, ARRLEN, ARRPOP, ARRTRIM, OBJKEYS, OBJLEN;

    private final static String PREFIX = "JSON.";

    public final byte[] bytes;

    CommandType() {
        bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
