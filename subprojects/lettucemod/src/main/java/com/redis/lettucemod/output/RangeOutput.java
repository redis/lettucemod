package com.redis.lettucemod.output;

import com.redis.lettucemod.api.timeseries.RangeResult;
import com.redis.lettucemod.api.timeseries.Sample;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.internal.LettuceStrings;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RangeOutput<K, V> extends CommandOutput<K, V, List<RangeResult<K, V>>> implements StreamingOutput<RangeResult<K, V>> {

    private boolean initialized;

    private Subscriber<RangeResult<K, V>> subscriber;

    private boolean skipKeyReset = false;

    private K key;

    private K labelKey;

    private Map<K, V> labels;

    private long timestamp;

    private List<Sample> samples;

    private boolean timestampReceived = false;

    private boolean labelsReceived = false;

    private boolean labelsComplete = false;

    private boolean samplesReceived = false;

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
            labelsReceived = true;

            if (bytes == null) {
                return;
            }

            labelKey = codec.decodeKey(bytes);
            return;
        }

        if (labels == null) {
            labels = new LinkedHashMap<>();
        }

        labels.put(labelKey, bytes == null ? null : codec.decodeValue(bytes));
        labelKey = null;
    }

    @Override
    public void set(long integer) {
        timestamp = integer;
        timestampReceived = true;
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
            samples.add(new Sample(timestamp, value));
        }
        timestamp = 0;
        timestampReceived = false;
    }

    @Override
    public void multi(int count) {

        if (labelsReceived && timestampReceived && count == -1) {
            samplesReceived = true;
        }

        if (key != null && labelKey == null && count == -1) {
            labelsReceived = true;
        }

        if (!initialized) {
            output = OutputFactory.newList(count);
            initialized = true;
        }
    }

    @Override
    public void complete(int depth) {

        if (depth == 2 && labelsReceived) {
            labelsComplete = true;
            return;
        }

        if (depth == 2 && samplesReceived) {
            subscriber.onNext(output, new RangeResult<>(key, labels == null ? Collections.emptyMap() : labels, samples == null ? Collections.emptyList() : samples));
            labelsReceived = false;
            labelsComplete = false;
            samplesReceived = false;
            labelKey = null;
            labels = null;
            timestampReceived = false;
            samples = null;
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
    public void setSubscriber(Subscriber<RangeResult<K, V>> subscriber) {
        LettuceAssert.notNull(subscriber, "Subscriber must not be null");
        this.subscriber = subscriber;
    }

    @Override
    public Subscriber<RangeResult<K, V>> getSubscriber() {
        return subscriber;
    }

}
