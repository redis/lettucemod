package com.redis.lettucemod.output;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceStrings;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.SearchResults;

public class SearchNoContentOutput<K, V> extends CommandOutput<K, V, SearchResults<K, V>> {

    private Document<K, V> current;
    private final boolean withScores;

    public SearchNoContentOutput(RedisCodec<K, V> codec, boolean withScores) {
        super(codec, new SearchResults<>());
        this.withScores = withScores;
    }

    @Override
    public void set(ByteBuffer bytes) {
        if (current == null) {
            current = new Document<>();
            if (bytes != null) {
                current.setId(codec.decodeKey(bytes));
            }
            output.add(current);
            if (!withScores) {
                current = null;
            }
        } else {
            if (withScores) {
                current.setScore(LettuceStrings.toDouble(decodeAscii(bytes)));
            }
            current = null;
        }
    }

    @Override
    public void set(long integer) {
        output.setCount(integer);
    }

    @Override
    public void set(double number) {
        if (withScores) {
            current.setScore(number);
        }
        current = null;
    }

}
