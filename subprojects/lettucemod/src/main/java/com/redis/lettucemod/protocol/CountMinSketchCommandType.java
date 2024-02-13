package com.redis.lettucemod.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

public enum CountMinSketchCommandType implements ProtocolKeyword {
    INCRBY, INFO, INITBYDIM, INITBYPROB, MERGE, QUERY;
    private static final String PREFIX = "CMS.";
    private final byte[] bytes;
    CountMinSketchCommandType(){bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);}

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
