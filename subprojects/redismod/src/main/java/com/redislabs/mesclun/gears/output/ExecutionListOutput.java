package com.redislabs.mesclun.gears.output;

import com.redislabs.mesclun.gears.Execution;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExecutionListOutput<K, V> extends CommandOutput<K, V, List<Execution>> implements StreamingOutput<Execution> {

    private boolean initialized;
    private Subscriber<Execution> subscriber;
    private Execution execution;
    private String field;

    public ExecutionListOutput(RedisCodec<K, V> codec) {
        super(codec, Collections.emptyList());
        setSubscriber(ListSubscriber.instance());
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (field == null) {
            field = decodeAscii(bytes);
            return;
        }
        if (field.equals("executionId")) {
            execution = new Execution();
            execution.setId(decodeAscii(bytes));
            field = null;
            return;
        }
        if (field.equals("status")) {
            execution.setStatus(decodeAscii(bytes));
            field = null;
        }
    }

    @Override
    public void set(long integer) {
        if (field.equals("registered")) {
            execution.setRegistered(integer);
            field = null;
            subscriber.onNext(output, execution);
            execution = null;
        }
    }

    @Override
    public void multi(int count) {
        if (!initialized) {
            output = newList(count);
            initialized = true;
        }
    }

    private List<Execution> newList(int capacity) {
        if (capacity < 1) {
            return Collections.emptyList();
        }
        return new ArrayList<>(capacity);
    }

    public void setSubscriber(Subscriber<Execution> subscriber) {
        this.subscriber = subscriber;
    }

    @Override
    public Subscriber<Execution> getSubscriber() {
        return subscriber;
    }

}
