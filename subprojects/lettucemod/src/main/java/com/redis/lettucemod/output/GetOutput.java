package com.redis.lettucemod.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceStrings;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.redis.lettucemod.timeseries.GetResult;
import com.redis.lettucemod.timeseries.Sample;

public class GetOutput<K, V> extends CommandOutput<K, V, List<GetResult<K, V>>>
		implements StreamingOutput<GetResult<K, V>> {

	private boolean initialized;

	private Subscriber<GetResult<K, V>> subscriber;

	private boolean skipKeyReset = false;

	private K key;

	private K labelKey;

	private Map<K, V> labels;

	private Sample sample;

	private boolean labelsComplete = false;

	public GetOutput(RedisCodec<K, V> codec) {
		super(codec, Collections.emptyList());
		setSubscriber(ListSubscriber.instance());
	}

	@Override
	public void set(ByteBuffer bytes) {

		if (key == null) {
			if (bytes == null) {
				return;
			}

			key = codec.decodeKey(bytes);
			skipKeyReset = true;
			return;
		}

		if (labelsComplete) {
			sampleValue(bytes == null ? null : LettuceStrings.toDouble(decodeAscii(bytes)));
			return;
		}

		if (labelKey == null) {

			if (bytes == null) {
				return;
			}

			labelKey = codec.decodeKey(bytes);
			return;
		}

		labels.put(labelKey, bytes == null ? null : codec.decodeValue(bytes));
		labelKey = null;
	}

	@Override
	public void set(long integer) {
		sample = new Sample();
		sample.setTimestamp(integer);
	}

	@Override
	public void set(double number) {
		sampleValue(number);
	}

	private void sampleValue(Double value) {
		sample.setValue(value);
		GetResult<K, V> result = new GetResult<>();
		result.setKey(key);
		result.setLabels(labels);
		result.setSample(sample);
		subscriber.onNext(output, result);
		labelsComplete = false;
		labelKey = null;
		labels = null;
		sample = null;
	}

	@Override
	public void multi(int count) {

		if (key != null && labels == null) {
			labels = new LinkedHashMap<>();
		}

		if (!initialized) {
			output = OutputFactory.newList(count);
			initialized = true;
		}
	}

	@Override
	public void complete(int depth) {

		if (depth == 2 && labels != null) {
			labelsComplete = true;
			return;
		}

		// RESP2/RESP3 compat
		if (depth == 2 && skipKeyReset) {
			skipKeyReset = false;
		}

		if (depth == 1) {
			if (skipKeyReset) {
				skipKeyReset = false;
			} else {
				key = null;
			}
		}
	}

	@Override
	public void setSubscriber(Subscriber<GetResult<K, V>> subscriber) {
		LettuceAssert.notNull(subscriber, "Subscriber must not be null");
		this.subscriber = subscriber;
	}

	@Override
	public Subscriber<GetResult<K, V>> getSubscriber() {
		return subscriber;
	}

}
