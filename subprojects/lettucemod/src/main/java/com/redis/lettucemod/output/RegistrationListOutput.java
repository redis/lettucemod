package com.redis.lettucemod.output;

import com.redis.lettucemod.api.gears.Registration;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

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
        if (fieldEquals("id")) {
            registration = new Registration();
            registration.setId(decodeAscii(bytes));
            field = null;
            return;
        }
        if (fieldEquals("reader")) {
            registration.setReader(decodeAscii(bytes));
            field = null;
            return;
        }
        if (fieldEquals("desc")) {
            registration.setDescription(decodeAscii(bytes));
            field = null;
            return;
        }
        if (fieldEquals("RegistrationData")) {
            registration.setData(new Registration.Data());
            field = decodeAscii(bytes);
            return;
        }
        if (registration.getData() != null) {
            if (fieldEquals("mode")) {
                registration.getData().setMode(decodeAscii(bytes));
                field = null;
                return;
            }
            if (fieldEquals("lastError")) {
                registration.getData().setLastError(decodeAscii(bytes));
                field = null;
                return;
            }
            if (fieldEquals("args")) {
                field = decodeAscii(bytes);
                return;
            }
            if (registration.getData().getArgs() != null && registration.getData().getArgs().size() < argSize) {
                registration.getData().getArgs().put(field, decodeAscii(bytes));
                field = null;
                return;
            }
            if (fieldEquals("status")) {
                registration.getData().setStatus(decodeAscii(bytes));
                field = null;
                return;
            }
        }
        if (fieldEquals("PD")) {
            registration.setPrivateData(decodeAscii(bytes));
            field = null;
            subscriber.onNext(output, registration);
            registration = null;
        }
    }

    private boolean fieldEquals(String name) {
        return name.equals(field);
    }

    @Override
    public void set(long integer) {
        if (registration.getData() != null) {
            if (fieldEquals("numTriggered")) {
                registration.getData().setNumTriggered(integer);
                field = null;
                return;
            }
            if (fieldEquals("numSuccess")) {
                registration.getData().setNumSuccess(integer);
                field = null;
                return;
            }
            if (fieldEquals("numFailures")) {
                registration.getData().setNumFailures(integer);
                field = null;
                return;
            }
            if (fieldEquals("numAborted")) {
                registration.getData().setNumAborted(integer);
                field = null;
                return;
            }
            if (registration.getData().getArgs() != null && registration.getData().getArgs().size() < argSize) {
                registration.getData().getArgs().put(field, integer);
                field = null;
            }
        }
    }

    @Override
    public void multi(int count) {
        if (!initialized) {
            output = OutputFactory.newList(count);
            initialized = true;
            return;
        }
        if (fieldEquals("args") && registration.getData() != null) {
            argSize = count / 2;
            registration.getData().setArgs(new HashMap<>(argSize));
        }
    }

    public void setSubscriber(Subscriber<Registration> subscriber) {
        this.subscriber = subscriber;
    }

    @Override
    public Subscriber<Registration> getSubscriber() {
        return subscriber;
    }

}
