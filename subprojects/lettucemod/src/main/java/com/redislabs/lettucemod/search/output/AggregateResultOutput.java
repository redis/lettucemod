package com.redislabs.lettucemod.search.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.*;

public class AggregateResultOutput<K, V> extends CommandOutput<K, V, Map<K, Object>> {

    private final List<V> array;
    private boolean initialized;
    private K key;
    private int count;
    private int expectedSize = -1;

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
                // Case where result array is empty
                if (count == 0) {
                    output.put(key, new ArrayList<>(array));
                    key = null;
                    array.clear();
                }
                this.count = count;
            }
        } else {
            expectedSize = count / 2;
            output = new LinkedHashMap<>(expectedSize, 1);
            initialized = true;
        }
    }


    public boolean isComplete() {
        return get().size() == expectedSize;
    }

    public Map<K, Object> getAndClear() {
        try {
            return new LinkedHashMap<>(get());
        } finally {
            get().clear();
            initialized = false;
            key = null;
            count = 0;
            expectedSize = -1;
        }
    }
}
