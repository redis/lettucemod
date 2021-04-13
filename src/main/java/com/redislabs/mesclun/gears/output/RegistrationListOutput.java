package com.redislabs.mesclun.gears.output;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.redislabs.mesclun.gears.Registration;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

public class RegistrationListOutput<K, V> extends CommandOutput<K, V, List<Registration>> implements StreamingOutput<Registration> {

    private boolean initialized;
    private Subscriber<Registration> subscriber;
    private Registration registration;
    private String field;
    private int argSize;

    public RegistrationListOutput(RedisCodec<K, V> codec) {
        super(codec, Collections.emptyList());
        setSubscriber(ListSubscriber.instance());
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (field == null) {
            field = decodeAscii(bytes);
            return;
        }
        if (field.equals("id")) {
            registration = new Registration();
            registration.setId(decodeAscii(bytes));
            field = null;
            return;
        }
        if (field.equals("reader")) {
            registration.setReader(decodeAscii(bytes));
            field = null;
            return;
        }
        if (field.equals("desc")) {
            registration.setDescription(decodeAscii(bytes));
            field = null;
            return;
        }
        if (field.equals("RegistrationData")) {
            registration.setData(new Registration.Data());
            field = decodeAscii(bytes);
            return;
        }
        if (field.equals("mode")) {
            registration.getData().setMode(decodeAscii(bytes));
            field = null;
            return;
        }
        if (field.equals("lastError")) {
            registration.getData().setLastError(decodeAscii(bytes));
            field = null;
            return;
        }
        if (field.equals("args")) {
            field = decodeAscii(bytes);
            return;
        }
        if (registration.getData().getArgs().size() < argSize) {
            registration.getData().getArgs().put(field, decodeAscii(bytes));
            field = null;
            return;
        }
        if (field.equals("status")) {
            registration.getData().setStatus(decodeAscii(bytes));
            field = null;
            return;
        }
        if (field.equals("PD")) {
            registration.setPrivateData(decodeAscii(bytes));
            field = null;
            subscriber.onNext(output, registration);
            registration = null;
        }
    }

    @Override
    public void set(long integer) {
        if (field.equals("numTriggered")) {
            registration.getData().setNumTriggered(integer);
            field = null;
            return;
        }
        if (field.equals("numSuccess")) {
            registration.getData().setNumSuccess(integer);
            field = null;
            return;
        }
        if (field.equals("numFailures")) {
            registration.getData().setNumFailures(integer);
            field = null;
            return;
        }
        if (field.equals("numAborted")) {
            registration.getData().setNumAborted(integer);
            field = null;
            return;
        }
        if (registration.getData().getArgs().size() < argSize) {
            registration.getData().getArgs().put(field, integer);
            field = null;
        }
    }

    @Override
    public void multi(int count) {
        if (!initialized) {
            output = newList(count);
            initialized = true;
            return;
        }
        if ("args".equals(field)) {
            argSize = count / 2;
            registration.getData().setArgs(new HashMap<>(argSize));
        }
    }

    private List<Registration> newList(int capacity) {
        if (capacity < 1) {
            return Collections.emptyList();
        }
        return new ArrayList<>(capacity);
    }

    public void setSubscriber(Subscriber<Registration> subscriber) {
        this.subscriber = subscriber;
    }

    @Override
    public Subscriber<Registration> getSubscriber() {
        return subscriber;
    }

}
