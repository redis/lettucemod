package com.redis.lettucemod.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceStrings;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.redis.lettucemod.search.Suggestion;

public class SuggetOutput<K, V> extends CommandOutput<K, V, List<Suggestion<V>>> {

    private final boolean withScores;
    private final boolean withPayloads;
    private Suggestion<V> current;
    private boolean payloadSet = false;
    private boolean scoreSet = false;

    public SuggetOutput(RedisCodec<K, V> codec) {
        this(codec, false, false);
    }

    public SuggetOutput(RedisCodec<K, V> codec, boolean withScores, boolean withPayloads) {
        super(codec, new ArrayList<>());
        this.withScores = withScores;
        this.withPayloads = withPayloads;
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (current == null) {
            current = new Suggestion<>();
            payloadSet = false;
            scoreSet = false;
            if (bytes != null) {
                current.setString(codec.decodeValue(bytes));
            }
            output.add(current);
            if (!withScores && !withPayloads) {
                current = null;
            }
        } else {
            if (withScores && !scoreSet) {
                current.setScore(LettuceStrings.toDouble(decodeAscii(bytes)));
                scoreSet = true;
                if (!withPayloads) {
                    current = null;
                }
            } else {
                if (withPayloads && !payloadSet) {
                    if (bytes != null) {
                        current.setPayload(codec.decodeValue(bytes));
                    }
                    payloadSet = true;
                    current = null;
                }
            }
        }
    }

    @Override
    public void set(double number) {
        if (withScores && !scoreSet) {
            current.setScore(number);
            scoreSet = true;
            if (!withPayloads) {
                current = null;
            }
        }
    }

}
