package com.redis.lettucemod.output;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.redis.lettucemod.timeseries.RangeResult;
import com.redis.lettucemod.timeseries.Sample;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceStrings;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

public class RangeOutput<K, V> extends CommandOutput<K, V, List<RangeResult<K, V>>>
		implements StreamingOutput<RangeResult<K, V>> {

	private Subscriber<RangeResult<K, V>> subscriber;
	private K key;
	private K labelKey;
	private Map<K, V> labels;
	private long timestamp;
	private List<Sample> samples;
	private boolean initialized;
	private boolean skipKeyReset;
	private boolean keyComplete;
	private boolean labelsComplete;

	public RangeOutput(RedisCodec<K, V> codec) {
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
			if (labels == null) {
				labels = new LinkedHashMap<>();
			}
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
		timestamp = integer;
	}

	@Override
	public void set(double number) {
		sampleValue(number);
	}

	private void sampleValue(Double value) {
		if (samples == null) {
			samples = new ArrayList<>();
		}
		if (value != null) {
			samples.add(Sample.of(timestamp, value));
		}
		timestamp = 0;
	}

	@Override
	public void multi(int count) {
		if (!initialized) {
			output = OutputFactory.newList(count);
			initialized = true;
		}
	}

	@Override
	public void complete(int depth) {
		if (depth == 2) {
			if (keyComplete) {
				if (labelsComplete) {
					labelsComplete();
				} else {
					labelsComplete = true;
				}
			} else {
				keyComplete = true;
			}
		} else {
			if (depth == 1) {
				if (skipKeyReset) {
					skipKeyReset = false;
				} else {
					key = null;
					keyComplete = false;
				}
			}
		}
	}

	private void labelsComplete() {
		RangeResult<K, V> result = new RangeResult<>();
		result.setKey(key);
		result.setLabels(labels == null ? Collections.emptyMap() : labels);
		result.setSamples(samples == null ? Collections.emptyList() : samples);
		subscriber.onNext(output, result);
		labelsComplete = false;
		labelKey = null;
		labels = null;
		samples = null;
		// RESP2/RESP3 compat
		if (skipKeyReset) {
			skipKeyReset = false;
		}
	}

	@Override
	public void setSubscriber(Subscriber<RangeResult<K, V>> subscriber) {
		LettuceAssert.notNull(subscriber, "Subscriber must not be null");
		this.subscriber = subscriber;
	}

	@Override
	public Subscriber<RangeResult<K, V>> getSubscriber() {
		return subscriber;
	}

}
