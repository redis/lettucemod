package com.redis.lettucemod.api.timeseries;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class KeySample<K> extends Sample {

    private K key;

    public static <K> KeySample<K> of(K key, long timestamp, double value) {
        KeySample<K> sample = new KeySample<>();
        sample.setKey(key);
        sample.setTimestamp(timestamp);
        sample.setValue(value);
        return sample;
    }
}
