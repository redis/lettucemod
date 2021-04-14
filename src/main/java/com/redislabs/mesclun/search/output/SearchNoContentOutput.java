package com.redislabs.mesclun.search.output;

import com.redislabs.mesclun.search.SearchResults;
import com.redislabs.mesclun.search.Document;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.CommandOutput;

import java.nio.ByteBuffer;

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
