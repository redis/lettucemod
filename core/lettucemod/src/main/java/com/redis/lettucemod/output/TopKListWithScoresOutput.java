package com.redis.lettucemod.output;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import io.lettuce.core.KeyValue;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

public class TopKListWithScoresOutput<K, V> extends CommandOutput<K, V, List<KeyValue<String, Long>>>
		implements StreamingOutput<KeyValue<String, Long>> {

	private String field;
	private boolean initalized;
	private Subscriber<KeyValue<String, Long>> subscriber;

	public TopKListWithScoresOutput(RedisCodec<K, V> codec) {
		super(codec, Collections.emptyList());
		setSubscriber(ListSubscriber.instance());
	}

	@Override
	public void set(ByteBuffer buffer) {
		field = decodeAscii(buffer);
	}

	@Override
	public void set(long integer) {
		subscriber.onNext(output, KeyValue.just(field, integer));
	}

	@Override
	public void multi(int count) {
		if (!initalized) {
			output = OutputFactory.newList(count);
			initalized = true;
		}
	}

	@Override
	public void setSubscriber(Subscriber<KeyValue<String, Long>> subscriber) {
		LettuceAssert.notNull(subscriber, "Subscriber must not be null");
		this.subscriber = subscriber;
	}

	@Override
	public Subscriber<KeyValue<String, Long>> getSubscriber() {
		return subscriber;
	}
}
