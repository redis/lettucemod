package com.redis.lettucemod.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

public enum BloomFilterCommandType implements ProtocolKeyword {
    ADD,CARD,EXISTS,INFO,INSERT,MADD,MEXISTS,RESERVE;
    private static final String PREFIX = "BF.";
    private final byte[] bytes;
    BloomFilterCommandType() {bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);}
    @Override
    public byte[] getBytes(){return bytes;}

}
