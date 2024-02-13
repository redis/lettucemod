package com.redis.lettucemod.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class OptionalValueListOutput <K,V> extends CommandOutput<K,V, List<Optional<V>>> implements StreamingOutput<Optional<V>> {
    private boolean initialized;
    private Subscriber<Optional<V>> subscriber;

    public OptionalValueListOutput(RedisCodec<K, V> codec) {
        super(codec, Collections.emptyList());
        setSubscriber(ListSubscriber.instance());
    }

    @Override
    public void set(ByteBuffer bytes){
        subscriber.onNext(output, bytes == null? Optional.empty() : Optional.of(codec.decodeValue(bytes)));

    }

    @Override
    public void multi(int count){
        if(!initialized){
            output = OutputFactory.newList(count);
            initialized = true;
        }
    }

    @Override
    public void setSubscriber(Subscriber<Optional<V>> subscriber) {
        LettuceAssert.notNull(subscriber, "Subscriber must not be null");
        this.subscriber = subscriber;

    }

    @Override
    public Subscriber<Optional<V>> getSubscriber() {
        return subscriber;
    }
}
