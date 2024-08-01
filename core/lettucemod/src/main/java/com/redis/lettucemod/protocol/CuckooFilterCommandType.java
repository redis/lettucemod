package com.redis.lettucemod.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

public enum CuckooFilterCommandType implements ProtocolKeyword {
    ADD,ADDNX,COUNT,DEL,EXISTS,INFO,INSERT,INSERTNX,MEXISTS,RESERVE;
    private static final String PREFIX = "CF.";
    private final byte[] bytes;
    CuckooFilterCommandType(){bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);}
    @Override
    public byte[] getBytes(){return bytes;}
}
