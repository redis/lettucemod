package com.redis.lettucemod.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

/**
 * RediSearch commands.
 *
 * @author Julien Ruaux
 */
public enum SearchCommandType implements ProtocolKeyword {

    AGGREGATE, ALTER, CREATE, CURSOR, DROPINDEX, INFO, SEARCH, SUGADD, SUGGET, SUGDEL, SUGLEN, ALIASADD, ALIASUPDATE, ALIASDEL, _LIST, TAGVALS, DICTADD, DICTDEL, DICTDUMP;

    private final static String PREFIX = "FT.";

    public final byte[] bytes;

    SearchCommandType() {
        bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
