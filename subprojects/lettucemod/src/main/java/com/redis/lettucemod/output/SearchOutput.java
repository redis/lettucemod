package com.redis.lettucemod.output;

import java.nio.ByteBuffer;

import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.SearchResults;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.internal.LettuceStrings;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.MapOutput;

public class SearchOutput<K, V> extends CommandOutput<K, V, SearchResults<K, V>> {

	private final boolean withScores;
	private final boolean withSortKeys;
	private final boolean withPayloads;
	private boolean sortKeySet = false;
	private boolean scoreSet = false;
	private boolean payloadSet = false;
	private MapOutput<K, V> contentOutput;
	private Document<K, V> currentDocument;

	public SearchOutput(RedisCodec<K, V> codec) {
		this(codec, false, false, false);
	}

	public SearchOutput(RedisCodec<K, V> codec, boolean withScores, boolean withSortKeys, boolean withPayloads) {
		super(codec, new SearchResults<>());
		this.withScores = withScores;
		this.withSortKeys = withSortKeys;
		this.withPayloads = withPayloads;
	}

	@Override
	public void set(ByteBuffer bytes) {
		if (currentDocument == null) {
			currentDocument = new Document<>();
			if (bytes != null) {
				currentDocument.setId(codec.decodeKey(bytes));
			}
			return;
		}
		if (withScores && !scoreSet) {
			if (bytes != null) {
				currentDocument.setScore(LettuceStrings.toDouble(decodeAscii(bytes)));
			}
			scoreSet = true;
			return;
		}
		if (withPayloads && !payloadSet) {
			if (bytes != null) {
				currentDocument.setPayload(codec.decodeValue(bytes));
			}
			payloadSet = true;
			return;
		}
		if (withSortKeys && !sortKeySet) {
			if (bytes != null) {
				currentDocument.setSortKey(codec.decodeValue(bytes));
			}
			sortKeySet = true;
			return;
		}
		if (contentOutput != null) {
			if (bytes != null) {
				contentOutput.set(bytes);
			}
			return;
		}
		if (bytes == null) {
			startNewDocument();
		}
	}

	@Override
	public void set(long integer) {
		output.setCount(integer);
	}

	@Override
	public void set(double number) {
		if (withScores && !scoreSet) {
			currentDocument.setScore(number);
			scoreSet = true;
		}
	}

	@Override
	public void complete(int depth) {
		if (contentOutput != null && depth == 1) {
			currentDocument.putAll(contentOutput.get());
			output.add(currentDocument);
			startNewDocument();
		}
	}

	private void startNewDocument() {
		currentDocument = null;
		contentOutput = null;
		payloadSet = false;
		scoreSet = false;
		sortKeySet = false;
	}

	@Override
	public void multi(int count) {
		if (currentDocument != null) {
			contentOutput = new MapOutput<>(codec);
			contentOutput.multi(count);
		}
	}

	@Override
	public boolean hasError() {
		return super.hasError() && !getError().startsWith("Success");
	}
}
