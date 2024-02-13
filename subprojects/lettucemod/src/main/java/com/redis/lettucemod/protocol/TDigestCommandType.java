package com.redis.lettucemod.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

public enum TDigestCommandType implements ProtocolKeyword {
    ADD, BYRANK, BYREVRANK, CDF, CREATE, INFO, MAX, MERGE, MIN, QUANTILE, RANK, RESET, REVRANK, TRIMMED_MEAN;
    private static final String PREFIX = "TDIGEST.";
    private final byte[] bytes;
    TDigestCommandType(){bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);}


    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
