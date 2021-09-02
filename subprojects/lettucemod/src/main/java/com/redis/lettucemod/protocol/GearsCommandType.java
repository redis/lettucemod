package com.redis.lettucemod.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

/**
 * RedisTimeSeries commands.
 *
 * @author Julien Ruaux
 */
public enum GearsCommandType implements ProtocolKeyword {

    ABORTEXECUTION, CONFIGGET, CONFIGSET, DROPEXECUTION, DUMPEXECUTIONS, DUMPREGISTRATIONS, GETEXECUTION, GETRESULTS, GETRESULTSBLOCKING, PYEXECUTE, TRIGGER, UNREGISTER;

    private final static String PREFIX = "RG.";

    public final byte[] bytes;

    GearsCommandType() {
        bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
