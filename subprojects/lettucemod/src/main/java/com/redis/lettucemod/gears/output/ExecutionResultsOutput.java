package com.redis.lettucemod.gears.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExecutionResultsOutput<K, V> extends CommandOutput<K, V, ExecutionResults> {

    private static final ByteBuffer OK = StandardCharsets.US_ASCII.encode("OK");

    private List<Object> current;
    private boolean initialized;

    public ExecutionResultsOutput(RedisCodec<K, V> codec) {
        super(codec, new ExecutionResults());
    }

    @Override
    public void set(ByteBuffer bytes) {

        // RESP 3 behavior
        if (bytes == null && !initialized) {
            return;
        }

        // function has no output => OK, execution does not exist/is still running => error
        if (current == null) {
            if (OK.equals(bytes)) {
                output.setOk(true);
            } else {
                output.setErrors(Collections.singletonList(decodeAscii(bytes)));
            }
            return;
        }

        current.add(codec.decodeValue(bytes));
    }

    @Override
    public void multi(int count) {
        if (!initialized) {
            initialized = true;
            return;
        }
        if (output.getResults() == null) {
            List<Object> results = new ArrayList<>(count);
            output.setResults(results);
            current = results;
            return;
        }
        output.setErrors(new ArrayList<>(count));
        current = new ArrayList<>(count);
    }


}
