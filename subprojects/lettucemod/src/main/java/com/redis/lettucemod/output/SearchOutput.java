package com.redis.lettucemod.output;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.SearchResults;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceStrings;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.MapOutput;

public class SearchOutput<K, V> extends CommandOutput<K, V, SearchResults<K, V>> {

	private final List<Integer> counts = new ArrayList<>();
	private final boolean withScores;
	private final boolean withSortKeys;
	private final boolean withPayloads;
	private MapOutput<K, V> nested;
	private Document<K, V> current;
	private int mapCount = -1;
	private boolean sortKeySet = false;
	private boolean scoreSet = false;
	private boolean payloadSet = false;

	public SearchOutput(RedisCodec<K, V> codec) {
		this(codec, false, false, false);
	}

	public SearchOutput(RedisCodec<K, V> codec, boolean withScores, boolean withSortKeys, boolean withPayloads) {
		super(codec, new SearchResults<>());
		nested = new MapOutput<>(codec);
		this.withScores = withScores;
		this.withSortKeys = withSortKeys;
		this.withPayloads = withPayloads;
	}

	@Override
	public void set(ByteBuffer bytes) {
		if (current == null) {
			current = new Document<>();
			payloadSet = false;
			scoreSet = false;
			sortKeySet = false;
			if (bytes != null) {
				current.setId(codec.decodeKey(bytes));
			}
			output.add(current);
			return;
		}
		if (withScores && !scoreSet) {
			current.setScore(LettuceStrings.toDouble(decodeAscii(bytes)));
			scoreSet = true;
			return;
		}
		if (withPayloads && !payloadSet) {
			if (bytes != null) {
				current.setPayload(codec.decodeValue(bytes));
			}
			payloadSet = true;
			return;
		}
		if (withSortKeys && !sortKeySet) {
			if (bytes != null) {
				current.setSortKey(codec.decodeValue(bytes));
			}
			sortKeySet = true;
			return;
		}
		if (bytes != null) {
			nested.set(bytes);
		}
	}

	@Override
	public void set(long integer) {
		output.setCount(integer);
	}

	@Override
	public void set(double number) {
		if (withScores && !scoreSet) {
			current.setScore(number);
			scoreSet = true;
		}
	}

	@Override
	public void complete(int depth) {
		if (counts.isEmpty()) {
			return;
		}
		if (nested.get().size() == counts.get(0)) {
			counts.remove(0);
			if (current != null) {
				current.putAll(nested.get());
			}
			nested = new MapOutput<>(codec);
			current = null;
			payloadSet = false;
			scoreSet = false;
			sortKeySet = false;
		}
	}

	@Override
	public void multi(int count) {
		nested.multi(count);
		if (mapCount == -1) {
			mapCount = count;
		} else {
			// div 2 because of key value pair counts twice
			counts.add(count / 2);
		}
	}

	@Override
	public boolean hasError() {
		return super.hasError() && !getError().startsWith("Success");
	}
}
