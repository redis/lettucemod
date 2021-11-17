package com.redis.lettucemod.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceStrings;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import com.redis.lettucemod.timeseries.Sample;

public class SampleListOutput<K, V> extends CommandOutput<K, V, List<Sample>> implements StreamingOutput<Sample> {

    private boolean initialized;

    private Subscriber<Sample> subscriber;

    private long timestamp;

    public SampleListOutput(RedisCodec<K, V> codec) {
        super(codec, Collections.emptyList());
        setSubscriber(ListSubscriber.instance());
    }

    @Override
    public void set(ByteBuffer bytes) {
        double value = LettuceStrings.toDouble(decodeAscii(bytes));
        set(value);
    }

    @Override
    public void set(long integer) {
        timestamp = integer;
    }

    @Override
    public void set(double number) {
        subscriber.onNext(output, new Sample(timestamp, number));
    }

    @Override
    public void multi(int count) {
        if (!initialized) {
            output = OutputFactory.newList(count);
            initialized = true;
        }
    }

    @Override
    public void setSubscriber(Subscriber<Sample> subscriber) {
        LettuceAssert.notNull(subscriber, "Subscriber must not be null");
        this.subscriber = subscriber;
    }

    @Override
    public Subscriber<Sample> getSubscriber() {
        return subscriber;
    }

}
