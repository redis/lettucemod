package com.redislabs.mesclun.search.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.*;

public class AggregateResultOutput<K, V> extends CommandOutput<K, V, Map<K, Object>> {

    private final List<V> array;
    private boolean initialized;
    private K key;
    private int count;

    public AggregateResultOutput(RedisCodec<K, V> codec) {
        super(codec, Collections.emptyMap());
        this.array = new ArrayList<>();
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (key == null) {
            key = (bytes == null) ? null : codec.decodeKey(bytes);
            return;
        }
        V value = (bytes == null) ? null : codec.decodeValue(bytes);
        if (count > 0) {
            array.add(value);
            if (array.size() == count) {
                output.put(key, new ArrayList<>(array));
                key = null;
                array.clear();
                count = 0;
            }
        } else {
            output.put(key, value);
            key = null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void set(long integer) {

        if (key == null) {
            key = (K) Long.valueOf(integer);
            return;
        }

        V value = (V) Long.valueOf(integer);
        output.put(key, value);
        key = null;
    }

    @Override
    public void multi(int count) {
        if (initialized) {
            if (key != null) {
                this.count = count;
            }
        } else {
            output = new LinkedHashMap<>(count / 2, 1);
            initialized = true;
        }
    }


}
