package com.redis.lettucemod.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

public enum TopKCommandType implements ProtocolKeyword {
    ADD,INCRBY,INFO,LIST,QUERY,RESERVE;
    private static final String PREFIX = "TOPK.";
    private final byte[] bytes;
    TopKCommandType(){bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);}

    @Override
    public byte[] getBytes() { return bytes; }
}
