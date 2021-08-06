package com.redislabs.lettucemod.gears.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

/**
 * RedisTimeSeries commands.
 *
 * @author Julien Ruaux
 */
public enum CommandType implements ProtocolKeyword {

    ABORTEXECUTION, CONFIGGET, CONFIGSET, DROPEXECUTION, DUMPEXECUTIONS, DUMPREGISTRATIONS, GETEXECUTION, GETRESULTS, GETRESULTSBLOCKING, PYEXECUTE, TRIGGER, UNREGISTER;

    private final static String PREFIX = "RG.";

    public final byte[] bytes;

    CommandType() {
        bytes = (PREFIX + name()).getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}
