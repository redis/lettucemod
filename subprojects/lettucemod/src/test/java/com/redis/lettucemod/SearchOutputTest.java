package com.redis.lettucemod;

import java.nio.ByteBuffer;

import com.redis.lettucemod.output.SearchOutput;
import com.redis.lettucemod.search.Document;
import com.redis.lettucemod.search.SearchResults;
import io.lettuce.core.codec.StringCodec;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

class SearchOutputTest {

	private SearchOutput<String, String> searchOutput = new SearchOutput<>(StringCodec.UTF8);

	@Test
	void parsesEmptyResponse() {
		searchOutput.multiArray(1);
		searchOutput.set(0);
		searchOutput.complete(1);
		searchOutput.complete(0);

		SearchResults<String, String> result = searchOutput.get();
		Assertions.assertTrue(result.isEmpty());
		Assertions.assertEquals(0, result.getCount());
	}

	@Test
	void parsesOneDocument() {
		searchOutput.multiArray(3);
		searchOutput.set(1);
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("key1".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.multi(4);
		searchOutput.set(ByteBuffer.wrap("hashKey1".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.set(ByteBuffer.wrap("hashValue1".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.set(ByteBuffer.wrap("hashKey2".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.set(ByteBuffer.wrap("hashValue2".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.complete(1);
		searchOutput.complete(0);

		SearchResults<String, String> result = searchOutput.get();
		Assertions.assertEquals(1, result.size());
		Assertions.assertEquals(1, result.getCount());
		Document<String, String> firstDocument = result.get(0);
		Assertions.assertEquals("key1", firstDocument.getId());
		Assertions.assertEquals(2, firstDocument.size());
		Assertions.assertTrue(firstDocument.containsKey("hashKey1"));
		Assertions.assertEquals("hashValue1", firstDocument.get("hashKey1"));
		Assertions.assertEquals("hashValue2", firstDocument.get("hashKey2"));
	}

	@Test
	void skipsDocumentWithNullContent() {
		searchOutput.multiArray(3);
		searchOutput.set(1);
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("key1".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.set(null);
		searchOutput.complete(1);
		searchOutput.complete(0);

		SearchResults<String, String> result = searchOutput.get();
		Assertions.assertTrue(result.isEmpty());
		assertEquals(1, result.getCount());
	}

	@Test
	void parsesDocumentAfterDocumentWithNullContent() {
		searchOutput.multiArray(5);
		searchOutput.set(2);
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("key1".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.set(null);
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("key2".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.multi(4);
		searchOutput.set(ByteBuffer.wrap("hashKey1".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.set(ByteBuffer.wrap("hashValue1".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.set(ByteBuffer.wrap("hashKey2".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.set(ByteBuffer.wrap("hashValue2".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.complete(1);
		searchOutput.complete(0);

		SearchResults<String, String> result = searchOutput.get();
		assertEquals(1, result.size());
		assertEquals(2, result.getCount());
		Document<String, String> firstDocument = result.get(0);
		assertEquals("key2", firstDocument.getId());
		assertEquals(2, firstDocument.size());
		assertEquals("hashValue1", firstDocument.get("hashKey1"));
		assertEquals("hashValue2", firstDocument.get("hashKey2"));
	}

	@Test
	void parsesWithScores_string() {
		searchOutput = new SearchOutput<>(StringCodec.UTF8, true, false, false);
		searchOutput.multiArray(4);
		searchOutput.set(1);
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("key".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("0.3".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.multiArray(2);
		searchOutput.set(ByteBuffer.wrap("hashKey".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.set(ByteBuffer.wrap("hashValue".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.complete(1);
		searchOutput.complete(0);

		SearchResults<String, String> result = searchOutput.get();
		assertEquals(1, result.size());
		assertEquals(1, result.getCount());
		Document<String, String> firstDocument = result.get(0);
		assertEquals("key", firstDocument.getId());
		assertEquals(0.3, firstDocument.getScore());
		assertEquals(1, firstDocument.size());
		assertEquals("hashValue", firstDocument.get("hashKey"));
	}

	@Test
	void parsesWithScores_double() {
		searchOutput = new SearchOutput<>(StringCodec.UTF8, true, false, false);
		searchOutput.multiArray(4);
		searchOutput.set(1);
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("key".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.set(0.3d);
		searchOutput.complete(1);
		searchOutput.multiArray(2);
		searchOutput.set(ByteBuffer.wrap("hashKey".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.set(ByteBuffer.wrap("hashValue".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.complete(1);
		searchOutput.complete(0);

		SearchResults<String, String> result = searchOutput.get();
		assertEquals(1, result.size());
		assertEquals(1, result.getCount());
		Document<String, String> firstDocument = result.get(0);
		assertEquals("key", firstDocument.getId());
		assertEquals(.3, firstDocument.getScore());
		assertEquals(1, firstDocument.size());
		assertEquals("hashValue", firstDocument.get("hashKey"));
	}

	@Test
	void parsesWithSortKeys() {
		searchOutput = new SearchOutput<>(StringCodec.UTF8, false, true, false);
		searchOutput.multiArray(4);
		searchOutput.set(1);
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("key".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("sortKey".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.multiArray(2);
		searchOutput.set(ByteBuffer.wrap("hashKey".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.set(ByteBuffer.wrap("hashValue".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.complete(1);
		searchOutput.complete(0);

		SearchResults<String, String> result = searchOutput.get();
		assertEquals(1, result.size());
		assertEquals(1, result.getCount());
		Document<String, String> firstDocument = result.get(0);
		assertEquals("key", firstDocument.getId());
		assertEquals("sortKey", firstDocument.getSortKey());
		assertEquals(1, firstDocument.size());
		assertEquals("hashValue", firstDocument.get("hashKey"));
	}

	@Test
	void parsesWithPayloads() {
		searchOutput = new SearchOutput<>(StringCodec.UTF8, false, false, true);
		searchOutput.multiArray(4);
		searchOutput.set(1);
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("key".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("payload".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.multiArray(2);
		searchOutput.set(ByteBuffer.wrap("hashKey".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.set(ByteBuffer.wrap("hashValue".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.complete(1);
		searchOutput.complete(0);

		SearchResults<String, String> result = searchOutput.get();
		assertEquals(1, result.size());
		assertEquals(1, result.getCount());
		Document<String, String> firstDocument = result.get(0);
		assertEquals("key", firstDocument.getId());
		assertEquals("payload", firstDocument.getPayload());
		assertEquals(1, firstDocument.size());
		assertEquals("hashValue", firstDocument.get("hashKey"));
	}

	@Test
	void parseWithScoresSortKeyAndPayload() {
		searchOutput = new SearchOutput<>(StringCodec.UTF8, true, true, true);
		searchOutput.multiArray(6);
		searchOutput.set(1);
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("key".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("0.6".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("payload".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.set(ByteBuffer.wrap("sortKey".getBytes(UTF_8)));
		searchOutput.complete(1);
		searchOutput.multiArray(2);
		searchOutput.set(ByteBuffer.wrap("hashKey".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.set(ByteBuffer.wrap("hashValue".getBytes(UTF_8)));
		searchOutput.complete(2);
		searchOutput.complete(1);
		searchOutput.complete(0);

		SearchResults<String, String> result = searchOutput.get();
		assertEquals(1, result.size());
		assertEquals(1, result.getCount());
		Document<String, String> firstDocument = result.get(0);
		assertEquals("key", firstDocument.getId());
		assertEquals(.6, firstDocument.getScore());
		assertEquals("sortKey", firstDocument.getSortKey());
		assertEquals("payload", firstDocument.getPayload());
		assertEquals(1, firstDocument.size());
		assertEquals("hashValue", firstDocument.get("hashKey"));
	}
}