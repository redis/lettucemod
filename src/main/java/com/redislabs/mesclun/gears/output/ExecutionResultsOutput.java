package com.redislabs.mesclun.gears.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ExecutionResultsOutput<K, V> extends CommandOutput<K, V, ExecutionResults> {

    private List current;
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

        // execution does not exist or is still running => error returned
        if (current == null) {
            output.setErrors(Collections.singletonList(decodeAscii(bytes)));
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
        List<String> errors = new ArrayList<>(count);
        output.setErrors(errors);
        current = errors;
    }


}
