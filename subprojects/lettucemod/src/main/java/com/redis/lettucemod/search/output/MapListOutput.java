/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redis.lettucemod.search.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.ListSubscriber;
import io.lettuce.core.output.StreamingOutput;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * {@link List} of {@link Map}s.
 *
 * @author Julien Ruaux
 * @since 1.4.3
 */
public class MapListOutput<K, V> extends CommandOutput<K, V, List<Map<K, V>>> implements StreamingOutput<Map<K, V>> {

    private boolean initialized;
    private Subscriber<Map<K, V>> subscriber;

    private K key;
    private Map<K, V> body;

    public MapListOutput(RedisCodec<K, V> codec) {
        super(codec, Collections.emptyList());
        setSubscriber(ListSubscriber.instance());
    }

    @Override
    public void set(ByteBuffer bytes) {

        if (key == null) {
            key = bytes == null ? null : codec.decodeKey(bytes);
            return;
        }

        if (body == null) {
            body = new LinkedHashMap<>();
        }

        body.put(key, bytes == null ? null : codec.decodeValue(bytes));
        key = null;
    }

    @Override
    public void multi(int count) {

        if (!initialized) {
            output = count > 0 ? new ArrayList<>(count) : Collections.emptyList();
            initialized = true;
        }
    }

    @Override
    public void complete(int depth) {

        if (depth == 1) {
            subscriber.onNext(output, body);
            key = null;
            body = null;
        }
    }

    @Override
    public void setSubscriber(Subscriber<Map<K, V>> subscriber) {
        LettuceAssert.notNull(subscriber, "Subscriber must not be null");
        this.subscriber = subscriber;
    }

    @Override
    public Subscriber<Map<K, V>> getSubscriber() {
        return subscriber;
    }
}
