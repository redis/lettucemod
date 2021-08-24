package com.redis.lettucemod.search.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

/**
 * RediSearch commands.
 *
 * @author Julien Ruaux
 */
public enum CommandType implements ProtocolKeyword {

    AGGREGATE, ALTER, CREATE, CURSOR, DROPINDEX, INFO, SEARCH, SUGADD, SUGGET, SUGDEL, SUGLEN, ALIASADD, ALIASUPDATE, ALIASDEL, _LIST, TAGVALS, DICTADD, DICTDEL, DICTDUMP;

    private final static String PREFIX = "FT.";

    public final byte[] bytes;

    CommandType() {
        bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
